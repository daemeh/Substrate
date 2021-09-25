//
//  RenderGraphContext.swift
//  
//
//  Created by Thomas Roughton on 21/06/20.
//

import SubstrateUtilities
import Foundation
import Dispatch

extension TaggedHeap.Tag {
    static var renderGraphResourceCommandArrayTag: Self {
        return 2807157891446559070
    }
}

final actor RenderGraphContextImpl<Backend: SpecificRenderBackend>: _RenderGraphContext {
    public let accessSemaphore: AsyncSemaphore?
       
    let backend: Backend
    let resourceRegistry: Backend.TransientResourceRegistry?
    let commandGenerator: ResourceCommandGenerator<Backend>
    let activeContextLock = AsyncSpinLock()
    
    var queueCommandBufferIndex: UInt64 = 0 // The last command buffer submitted
    let syncEvent: Backend.Event
       
    let commandQueue: Backend.QueueImpl
       
    public let transientRegistryIndex: Int
    let renderGraphQueue: Queue
    
    var compactedResourceCommands = [CompactedResourceCommand<Backend.CompactedResourceCommandType>]()
       
    var enqueuedEmptyFrameCompletionSemaphores = [(UInt64, AsyncSemaphore)]()
    
    public init(backend: Backend, inflightFrameCount: Int, transientRegistryIndex: Int) {
        self.backend = backend
        self.renderGraphQueue = Queue()
        self.commandQueue = backend.makeQueue(renderGraphQueue: self.renderGraphQueue)
        self.transientRegistryIndex = transientRegistryIndex
        self.resourceRegistry = inflightFrameCount > 0 ? backend.makeTransientRegistry(index: transientRegistryIndex, inflightFrameCount: inflightFrameCount, queue: self.renderGraphQueue) : nil
        self.accessSemaphore = inflightFrameCount > 0 ? AsyncSemaphore(value: Int32(inflightFrameCount)) : nil
        
        self.commandGenerator = ResourceCommandGenerator()
        self.syncEvent = backend.makeSyncEvent(for: self.renderGraphQueue)
    }
    
    deinit {
        backend.freeSyncEvent(for: self.renderGraphQueue)
        self.renderGraphQueue.dispose()
        self.accessSemaphore?.deinit()
    }
    
    func registerWindowTexture(for texture: Texture, swapchain: Any) async {
        guard let resourceRegistry = self.resourceRegistry else {
            print("Error: cannot associate a window texture with a no-transient-resources RenderGraph")
            return
        }

        await resourceRegistry.registerWindowTexture(for: texture, swapchain: swapchain)
    }
    
    nonisolated var resourceMap : FrameResourceMap<Backend> {
        return FrameResourceMap<Backend>(persistentRegistry: self.backend.resourceRegistry, transientRegistry: self.resourceRegistry)
    }
    
    func processEmptyFrameCompletionHandlers(afterSubmissionIndex: UInt64) {
        // Notify any completion handlers that were enqueued for frames with no work.
        while let (afterCBIndex, completionSemaphore) = self.enqueuedEmptyFrameCompletionSemaphores.first {
            if afterCBIndex > afterSubmissionIndex { break }
            completionSemaphore.signal()
            self.accessSemaphore?.signal()
            self.enqueuedEmptyFrameCompletionSemaphores.removeFirst()
        }
    }
    
    
    func submitCommandBuffer(_ commandBuffer: Backend.CommandBuffer, commandBufferIndex: Int, lastCommandBufferIndex: Int, executionResult: RenderGraphExecutionResult, taskCompletionSemaphore: AsyncSemaphore, syncEvent: Backend.Event) async {
        // Make sure that the sync event value is what we expect, so we don't update it past
        // the signal for another buffer before that buffer has completed.
        // We only need to do this if we haven't already waited in this command buffer for it.
        // if commandEncoderWaitEventValues[commandEncoderIndex] != self.queueCommandBufferIndex {
        //     commandBuffer.encodeWaitForEvent(self.syncEvent, value: self.queueCommandBufferIndex)
        // }
        // Then, signal our own completion.
        self.queueCommandBufferIndex += 1
        commandBuffer.signalEvent(syncEvent, value: self.queueCommandBufferIndex)
        
        let queueCBIndex = self.queueCommandBufferIndex
        
        let returnedCommandBufferIndex = await self.renderGraphQueue.submitCommand(completionTask: Task {
            await withUnsafeContinuation({ continuation in
                commandBuffer.commit(onCompletion: { (commandBuffer) in
                    if let error = commandBuffer.error {
                        print("Error executing command buffer \(queueCBIndex): \(error)")
                    }
                    
                    CommandEndActionManager.didCompleteCommand(queueCBIndex, on: self.renderGraphQueue)
                    
                    if commandBufferIndex == 0 {
                        executionResult.gpuTime = commandBuffer.gpuStartTime
                    }
                    if commandBufferIndex == lastCommandBufferIndex { // Only call completion for the last command buffer.
                        let gpuEndTime = commandBuffer.gpuEndTime
                        
                        executionResult.gpuTime = (gpuEndTime - executionResult.gpuTime) * 1000.0
                        taskCompletionSemaphore.signal()
                        self.backend.didCompleteCommand(queueCBIndex, queue: self.renderGraphQueue, context: self)
                        
                        self.accessSemaphore?.signal()
                        
                        self.processEmptyFrameCompletionHandlers(afterSubmissionIndex: queueCBIndex)
                    }
                    
                    continuation.resume()
                })
            })
        })
        
        assert(queueCBIndex == returnedCommandBufferIndex)
    }
    
    func executeRenderGraph(_ executeFunc: () async -> (passes: [RenderPassRecord], usedResources: Set<Resource>)) async -> Task<RenderGraphExecutionResult, Never> {
        await self.accessSemaphore?.wait()
        await self.backend.reloadShaderLibraryIfNeeded()
        await self.activeContextLock.lock()
        defer {
            self.activeContextLock.unlock()
        }
        
        return await Backend.activeContextTaskLocal.withValue(self) {
            let (passes, usedResources) = await executeFunc()
            
            // Use separate command buffers for onscreen and offscreen work (Delivering Optimised Metal Apps and Games, WWDC 2019)
            self.resourceRegistry?.prepareFrame()
            
            let taskCompletionSemaphore = AsyncSemaphore(value: 0)
            let executionResult = RenderGraphExecutionResult()
            
            if passes.isEmpty {
                self.enqueuedEmptyFrameCompletionSemaphores.append((self.queueCommandBufferIndex, taskCompletionSemaphore))

                if self.renderGraphQueue.lastCompletedCommand >= self.renderGraphQueue.lastSubmittedCommand {
                    self.accessSemaphore?.signal()
                    taskCompletionSemaphore.signal()
                }
                return detach { [executionResult] in
                    await taskCompletionSemaphore.wait()
                    taskCompletionSemaphore.deinit()
                    return executionResult
                }
            }
            
            var frameCommandInfo = FrameCommandInfo<Backend>(passes: passes, initialCommandBufferSignalValue: self.queueCommandBufferIndex + 1)
            self.commandGenerator.generateCommands(passes: passes, usedResources: usedResources, transientRegistry: self.resourceRegistry, backend: backend, frameCommandInfo: &frameCommandInfo)
            await self.commandGenerator.executePreFrameCommands(context: self, frameCommandInfo: &frameCommandInfo)
            
            await RenderGraph.signposter.withIntervalSignpost("Sort and Compact Resource Commands") {
                self.commandGenerator.commands.sort() // We do this here since executePreFrameCommands may have added to the commandGenerator commands.
                
                var compactedResourceCommands = self.compactedResourceCommands // Re-use its storage
                self.compactedResourceCommands = []
                await backend.compactResourceCommands(queue: self.renderGraphQueue, resourceMap: self.resourceMap, commandInfo: frameCommandInfo, commandGenerator: self.commandGenerator, into: &compactedResourceCommands)
                self.compactedResourceCommands = compactedResourceCommands
            }
            
            var commandBuffers = [Backend.CommandBuffer]()
            var waitedEvents = QueueCommandIndices(repeating: 0)
            
            for (i, encoderInfo) in frameCommandInfo.commandEncoders.enumerated() {
                let commandBufferIndex = encoderInfo.commandBufferIndex
                if commandBufferIndex != commandBuffers.endIndex - 1 {
                    if let transientRegistry = resourceMap.transientRegistry {
                        commandBuffers.last?.presentSwapchains(resourceRegistry: transientRegistry)
                    }
                    commandBuffers.append(self.commandQueue.makeCommandBuffer(commandInfo: frameCommandInfo,
                                                                              resourceMap: resourceMap,
                                                                              compactedResourceCommands: self.compactedResourceCommands))
                }
                
                let waitEventValues = encoderInfo.queueCommandWaitIndices
                for queue in QueueRegistry.allQueues {
                    if waitedEvents[Int(queue.index)] < waitEventValues[Int(queue.index)],
                        waitEventValues[Int(queue.index)] > queue.lastCompletedCommand {
                        if let event = backend.syncEvent(for: queue) {
                            commandBuffers.last!.waitForEvent(event, value: waitEventValues[Int(queue.index)])
                        } else {
                            // It's not a queue known to this backend, so the best we can do is sleep and wait until the queue is completd.
                            await queue.waitForCommandCompletion(waitEventValues[Int(queue.index)])
                        }
                    }
                }
                waitedEvents = pointwiseMax(waitEventValues, waitedEvents)
                
                await RenderGraph.signposter.withIntervalSignpost("Encode to Command Buffer", "Encode commands for command encoder \(i)") {
                    await commandBuffers.last!.encodeCommands(encoderIndex: i)
                }
            }
            
            for passRecord in passes {
                passRecord.pass = nil // Release references to the RenderPasses.
            }
            
            TaggedHeap.free(tag: .renderGraphResourceCommandArrayTag)
            
            self.resourceRegistry?.cycleFrames()
            self.commandGenerator.reset()
            self.compactedResourceCommands.removeAll(keepingCapacity: true)
            
            let syncEvent = backend.syncEvent(for: self.renderGraphQueue)!
            for (i, commandBuffer) in commandBuffers.enumerated() {
                await self.submitCommandBuffer(commandBuffer, commandBufferIndex: i, lastCommandBufferIndex: commandBuffers.count - 1, executionResult: executionResult, taskCompletionSemaphore: taskCompletionSemaphore, syncEvent: syncEvent)
            }
            
            return detach { [executionResult] in
                await taskCompletionSemaphore.wait()
                taskCompletionSemaphore.deinit()
                return executionResult
            }
        }
    }
}
