//
//  VkFrameGraphContext.swift
//  VkRenderer
//
//  Created by Joseph Bennett on 2/01/18.
//

#if canImport(Vulkan)
import Vulkan
import Dispatch
import FrameGraphCExtras
import FrameGraphUtilities

extension VulkanBackend {
    func generateEventCommands(queue: Queue, resourceMap: FrameResourceMap<VulkanBackend>, frameCommandInfo: FrameCommandInfo<VulkanBackend>, commandGenerator: ResourceCommandGenerator<VulkanBackend>, compactedResourceCommands: inout [CompactedResourceCommand<VulkanCompactedResourceCommandType>]) {
        // MARK: - Generate the events
        
        let dependencies: DependencyTable<FineDependency?> = commandGenerator.commandEncoderDependencies
        
        let commandEncoderCount = frameCommandInfo.commandEncoders.count
        let reductionMatrix = dependencies.transitiveReduction(hasDependency: { $0 != nil })
        
        let allocator = ThreadLocalTagAllocator(tag: FrameGraphContextImpl<VulkanBackend>.resourceCommandArrayTag)
        
        for sourceIndex in (0..<commandEncoderCount) { // sourceIndex always points to the producing pass.
            let dependentRange = min(sourceIndex + 1, commandEncoderCount)..<commandEncoderCount
            
            var signalStages: VkPipelineStageFlagBits = []
            var signalIndex = -1
            for dependentIndex in dependentRange where reductionMatrix.dependency(from: dependentIndex, on: sourceIndex) {
                let dependency = dependencies.dependency(from: dependentIndex, on: sourceIndex)!
                
                for (resource, producingUsage, _) in dependency.resources {
                    let pixelFormat = resource.texture?.descriptor.pixelFormat ?? .invalid
                    let isDepthOrStencil = pixelFormat.isDepth || pixelFormat.isStencil
                    signalStages.formUnion(producingUsage.type.shaderStageMask(isDepthOrStencil: isDepthOrStencil, stages: producingUsage.stages))
                }
                
                signalIndex = max(signalIndex, dependency.signal.index)
            }
            
            if signalIndex < 0 { continue }
            
            let label = "Encoder \(sourceIndex) Event"
            let sourceEncoder = frameCommandInfo.commandEncoders[sourceIndex]
            let commandBufferSignalValue = frameCommandInfo.signalValue(commandBufferIndex: sourceEncoder.commandBufferIndex)
            let fence = VulkanEventHandle(label: label, queue: queue, commandBufferIndex: commandBufferSignalValue)

            if sourceEncoder.type == .draw {
                signalIndex = max(signalIndex, sourceEncoder.commandRange.last!) // We can't signal within a VkRenderPass instance.
            }
            
            compactedResourceCommands.append(CompactedResourceCommand<VulkanCompactedResourceCommandType>(command: .signalEvent(fence.event, afterStages: signalStages), index: signalIndex, order: .after))
            
            for dependentIndex in dependentRange where reductionMatrix.dependency(from: dependentIndex, on: sourceIndex) {
                let dependency = dependencies.dependency(from: dependentIndex, on: sourceIndex)!
                var destinationStages: VkPipelineStageFlagBits = []
                
                var bufferBarriers = [VkBufferMemoryBarrier]()
                var imageBarriers = [VkImageMemoryBarrier]()
                
//                assert(self.device.queueFamilyIndex(queue: queue, encoderType: sourceEncoderType) == self.device.queueFamilyIndex(queue: queue, encoderType: destinationEncoderType), "Queue ownership transfers must be handled with a pipeline barrier rather than an event")
                
                for (resource, producingUsage, consumingUsage) in dependency.resources {
                    var isDepthOrStencil = false
                    
                    if let buffer = resource.buffer {
                        var barrier = VkBufferMemoryBarrier()
                        barrier.sType = VK_STRUCTURE_TYPE_BUFFER_MEMORY_BARRIER
                        barrier.buffer = resourceMap[buffer].buffer.vkBuffer
                        barrier.offset = 0
                        barrier.size = VK_WHOLE_SIZE // TODO: track at a more fine-grained level.
                        barrier.srcAccessMask = producingUsage.type.accessMask(isDepthOrStencil: false).rawValue
                        barrier.dstAccessMask = consumingUsage.type.accessMask(isDepthOrStencil: false).rawValue
                        barrier.srcQueueFamilyIndex = VK_QUEUE_FAMILY_IGNORED
                        barrier.dstQueueFamilyIndex = VK_QUEUE_FAMILY_IGNORED
                        bufferBarriers.append(barrier)
                    } else if let texture = resource.texture {
                        let pixelFormat = texture.descriptor.pixelFormat
                        isDepthOrStencil = pixelFormat.isDepth || pixelFormat.isStencil
                        
                        let image = resource.texture.map({ resourceMap[$0].image })!
                        
                        var barrier = VkImageMemoryBarrier()
                        barrier.sType = VK_STRUCTURE_TYPE_IMAGE_MEMORY_BARRIER
                        barrier.image = resourceMap[texture].image.vkImage
                        barrier.srcAccessMask = producingUsage.type.accessMask(isDepthOrStencil: isDepthOrStencil).rawValue
                        barrier.dstAccessMask = consumingUsage.type.accessMask(isDepthOrStencil: isDepthOrStencil).rawValue
                        barrier.srcQueueFamilyIndex = VK_QUEUE_FAMILY_IGNORED
                        barrier.dstQueueFamilyIndex = VK_QUEUE_FAMILY_IGNORED
                        barrier.oldLayout = image.layout(commandIndex: producingUsage.commandRange.last!).afterOperation
                        barrier.newLayout = image.layout(commandIndex: consumingUsage.commandRange.first!).beforeOperation
                        if producingUsage.type.isRenderTarget {
                            // We transitioned to the new layout at the end of the previous render pass.
                            barrier.oldLayout = barrier.newLayout 
                        } else if consumingUsage.type.isRenderTarget {
                            // The layout transition will be handled by the next render pass.
                            barrier.newLayout = barrier.oldLayout
                        } 
                        barrier.subresourceRange = VkImageSubresourceRange(aspectMask: texture.descriptor.pixelFormat.aspectFlags, baseMipLevel: 0, levelCount: UInt32(texture.descriptor.mipmapLevelCount), baseArrayLayer: 0, layerCount: UInt32(texture.descriptor.arrayLength))
                        imageBarriers.append(barrier)
                    } else {
                        fatalError()
                    }
                    
                    destinationStages.formUnion(consumingUsage.type.shaderStageMask(isDepthOrStencil: isDepthOrStencil, stages: consumingUsage.stages))
                }
                
                let bufferBarriersPtr: UnsafeMutablePointer<VkBufferMemoryBarrier> = allocator.allocate(capacity: bufferBarriers.count)
                bufferBarriersPtr.initialize(from: bufferBarriers, count: bufferBarriers.count)
                
                let imageBarriersPtr: UnsafeMutablePointer<VkImageMemoryBarrier> = allocator.allocate(capacity: imageBarriers.count)
                imageBarriersPtr.initialize(from: imageBarriers, count: imageBarriers.count)
                
                let command: VulkanCompactedResourceCommandType = .waitForEvents(UnsafeBufferPointer(start: fence.eventPointer, count: 1),
                                                                                 sourceStages: signalStages, destinationStages: destinationStages,
                                                                                 memoryBarriers: UnsafeBufferPointer<VkMemoryBarrier>(start: nil, count: 0),
                                                                                 bufferMemoryBarriers: UnsafeBufferPointer<VkBufferMemoryBarrier>(start: bufferBarriersPtr, count: bufferBarriers.count),
                                                                                 imageMemoryBarriers: UnsafeBufferPointer<VkImageMemoryBarrier>(start: imageBarriersPtr, count: imageBarriers.count))
                
                var waitIndex = dependency.wait.index
                let dependentEncoder = frameCommandInfo.commandEncoders[dependentIndex]
                if dependentEncoder.type == .draw {
                    waitIndex = dependentEncoder.commandRange.first! // We can't wait within a VkRenderPass instance.
                }

                compactedResourceCommands.append(CompactedResourceCommand<VulkanCompactedResourceCommandType>(command: command, index: waitIndex, order: .before))
            }
        }
    }
    
    func compactResourceCommands(queue: Queue, resourceMap: FrameResourceMap<VulkanBackend>, commandInfo: FrameCommandInfo<VulkanBackend>, commandGenerator: ResourceCommandGenerator<VulkanBackend>, into compactedResourceCommands: inout [CompactedResourceCommand<VulkanCompactedResourceCommandType>]) {
        
        guard !commandGenerator.commands.isEmpty else { return }
        assert(compactedResourceCommands.isEmpty)
        
        self.generateEventCommands(queue: queue, resourceMap: resourceMap, frameCommandInfo: commandInfo, commandGenerator: commandGenerator, compactedResourceCommands: &compactedResourceCommands)
        
        
        let allocator = ThreadLocalTagAllocator(tag: FrameGraphContextImpl<VulkanBackend>.resourceCommandArrayTag)
        
        var currentEncoderIndex = 0
        var currentEncoder = commandInfo.commandEncoders[currentEncoderIndex]
        var currentPassIndex = 0
        
        var bufferBarriers = [VkBufferMemoryBarrier]()
        var imageBarriers = [VkImageMemoryBarrier]()
        
        var barrierAfterStages: VkPipelineStageFlagBits = []
        var barrierBeforeStages: VkPipelineStageFlagBits = []
        var barrierLastIndex: Int = .max
        
        let addBarrier: (inout [CompactedResourceCommand<VulkanCompactedResourceCommandType>]) -> Void = { compactedResourceCommands in
            let bufferBarriersPtr: UnsafeMutablePointer<VkBufferMemoryBarrier> = allocator.allocate(capacity: bufferBarriers.count)
            bufferBarriersPtr.initialize(from: bufferBarriers, count: bufferBarriers.count)
            
            let imageBarriersPtr: UnsafeMutablePointer<VkImageMemoryBarrier> = allocator.allocate(capacity: imageBarriers.count)
            imageBarriersPtr.initialize(from: imageBarriers, count: imageBarriers.count)
            
            let command: VulkanCompactedResourceCommandType = .pipelineBarrier(sourceStages: barrierAfterStages,
                                                                               destinationStages: barrierBeforeStages,
                                                                               dependencyFlags: VkDependencyFlagBits(rawValue: 0),
                                                                               memoryBarriers: UnsafeBufferPointer<VkMemoryBarrier>(start: nil, count: 0),
                                                                               bufferMemoryBarriers: UnsafeBufferPointer<VkBufferMemoryBarrier>(start: bufferBarriersPtr, count: bufferBarriers.count),
                                                                               imageMemoryBarriers: UnsafeBufferPointer<VkImageMemoryBarrier>(start: imageBarriersPtr, count: imageBarriers.count))
            
            compactedResourceCommands.append(.init(command: command, index: barrierLastIndex, order: .before))

            bufferBarriers.removeAll(keepingCapacity: true)
            imageBarriers.removeAll(keepingCapacity: true)
            barrierAfterStages = []
            barrierBeforeStages = []
            barrierLastIndex = .max
        }
        
        func processMemoryBarrier(resource: Resource, afterCommand: Int, afterUsage: ResourceUsageType, afterStages: RenderStages, beforeCommand: Int, beforeUsage: ResourceUsageType, beforeStages: RenderStages) {
            let pixelFormat =  resource.texture?.descriptor.pixelFormat ?? .invalid
            let isDepthOrStencil = pixelFormat.isDepth || pixelFormat.isStencil

            let sourceLayout: VkImageLayout
            let destinationLayout: VkImageLayout
            if let image = resource.texture.map({ resourceMap[$0].image }) {
                sourceLayout = afterUsage == .previousFrame ? image.frameInitialLayout : image.layout(commandIndex: afterCommand).afterOperation
                destinationLayout = image.layout(commandIndex: beforeCommand).beforeOperation
            } else {
                assert(resource.type != .texture || resource.flags.contains(.windowHandle))
                sourceLayout = VK_IMAGE_LAYOUT_UNDEFINED
                destinationLayout = VK_IMAGE_LAYOUT_UNDEFINED
            }
            if sourceLayout == destinationLayout, afterUsage == .previousFrame {
                return // No layout transition needed, so we don't need a memory barrier.
            }
            
            let sourceMask = afterUsage.shaderStageMask(isDepthOrStencil: isDepthOrStencil, stages: afterStages)
            let destinationMask = beforeUsage.shaderStageMask(isDepthOrStencil: isDepthOrStencil, stages: beforeStages)
            
            let sourceAccessMask = afterUsage.accessMask(isDepthOrStencil: isDepthOrStencil).rawValue
            let destinationAccessMask = beforeUsage.accessMask(isDepthOrStencil: isDepthOrStencil).rawValue

            var beforeCommand = beforeCommand
            var subresourceRanges: [VkImageSubresourceRange] = []
            
            if let renderTargetDescriptor = currentEncoder.renderTargetDescriptor, beforeCommand > currentEncoder.commandRange.lowerBound {
                var subpassDependency = VkSubpassDependency()
                subpassDependency.dependencyFlags = 0 // FIXME: ideally should be VkDependencyFlags(VK_DEPENDENCY_BY_REGION_BIT) for all cases except temporal AA.
                if afterUsage == .previousFrame {
                    subpassDependency.srcSubpass = VK_SUBPASS_EXTERNAL
                } else if let passUsageSubpass = renderTargetDescriptor.subpassForPassIndex(currentPassIndex) {
                    subpassDependency.srcSubpass = UInt32(passUsageSubpass.index)
                } else {
                    subpassDependency.srcSubpass = VK_SUBPASS_EXTERNAL
                }
                subpassDependency.srcStageMask = sourceMask.rawValue
                subpassDependency.srcAccessMask = sourceAccessMask
                
                let dependentPass = commandInfo.passes[currentPassIndex...].first(where: { $0.commandRange!.contains(beforeCommand) })!
                if let destinationUsageSubpass = renderTargetDescriptor.subpassForPassIndex(dependentPass.passIndex) {
                    subpassDependency.dstSubpass = UInt32(destinationUsageSubpass.index)
                } else {
                    subpassDependency.dstSubpass = VK_SUBPASS_EXTERNAL
                }
                subpassDependency.dstStageMask = destinationMask.rawValue
                subpassDependency.dstAccessMask = destinationAccessMask

                // If the dependency is on an attachment, then we can let the subpass dependencies handle it, _unless_ both usages are in the same subpass.
                // Otherwise, an image should always be in the right layout when it's materialised. The only case it won't be is if it's used in one way in
                // a draw render pass (e.g. as a read texture) and then needs to transition layout before being used in a different type of pass.

                if subpassDependency.srcSubpass == subpassDependency.dstSubpass {
                    precondition(resource.type == .texture, "We can only insert pipeline barriers within render passes for textures.")
                    assert(subpassDependency.srcSubpass != VK_SUBPASS_EXTERNAL, "Dependent pass \(dependentPass.passIndex): Subpass dependency from \(afterUsage) (afterCommand \(afterCommand)) to \(beforeUsage) (beforeCommand \(beforeCommand)) for resource \(resource) is EXTERNAL to EXTERNAL, which is invalid.")
                    renderTargetDescriptor.addDependency(subpassDependency)
                } else if sourceLayout != destinationLayout, // guaranteed to not be a buffer since buffers have UNDEFINED image layouts above.
                            !afterUsage.isRenderTarget, !beforeUsage.isRenderTarget {
                    // We need to insert a pipeline barrier to handle a layout transition.
                    // We can therefore avoid a subpass dependency in most cases.

                    if subpassDependency.srcSubpass == VK_SUBPASS_EXTERNAL {
                        // Insert a pipeline barrier before the start of the Render Command Encoder.
                        beforeCommand = min(beforeCommand, currentEncoder.commandRange.lowerBound)
                    } else if subpassDependency.dstSubpass == VK_SUBPASS_EXTERNAL {
                        // Insert a pipeline barrier before the next command after the render command encoder ends.
                        assert(beforeCommand >= currentEncoder.commandRange.last!)
                    } else {
                        // Insert a subpass self-dependency and a pipeline barrier.
                        fatalError("This should have been handled by the subpassDependency.srcSubpass == subpassDependency.dstSubpass case.")

                        // resourceCommands.append(VulkanFrameResourceCommand(command: .pipelineBarrier(memoryBarrierInfo), index: dependentCommandIndex, order: .before))
                        // subpassDependency.srcSubpass = subpassDependency.dstSubpass
                        // renderTargetDescriptor.addDependency(subpassDependency)
                    }
                } else {
                    // A subpass dependency should be enough to handle this case, unless there are other layers we also need to transition.
                    // TODO: can we do better with fine-grained tracking of layouts and barriers for different layers?
                    renderTargetDescriptor.addDependency(subpassDependency)
                    
                    if sourceLayout != destinationLayout, let texture = resource.texture {
                        let mipCount = texture.descriptor.mipmapLevelCount
                        let arrayLength = texture.descriptor.arrayLength
                        
                        if mipCount > 1 || arrayLength > 1 {
                            // Compute which slices of the texture were not rendered to (and therefore need a manual layout transition).
                            let pixelFormat = texture.descriptor.pixelFormat
                            var usedSlices: [(level: Int, slice: Int, depthPlane: Int)] = []
                            
                            if !(pixelFormat.isDepth || pixelFormat.isStencil) {
                                for attachment in renderTargetDescriptor.descriptor.colorAttachments {
                                    guard let attachment = attachment else { continue }
                                    if attachment.texture == texture {
                                        usedSlices.append((attachment.level, attachment.slice, attachment.depthPlane))
                                    }
                                    if attachment.resolveTexture == texture {
                                        usedSlices.append((attachment.resolveLevel, attachment.resolveSlice, attachment.resolveDepthPlane))
                                    }
                                }
                            }
                            
                            if pixelFormat.isDepth, let attachment = renderTargetDescriptor.descriptor.depthAttachment {
                                if attachment.texture == texture {
                                    usedSlices.append((attachment.level, attachment.slice, attachment.depthPlane))
                                }
                                if attachment.resolveTexture == texture {
                                    usedSlices.append((attachment.resolveLevel, attachment.resolveSlice, attachment.resolveDepthPlane))
                                }
                            }
                            
                            if pixelFormat.isStencil, let attachment = renderTargetDescriptor.descriptor.stencilAttachment {
                                if attachment.texture == texture {
                                    usedSlices.append((attachment.level, attachment.slice, attachment.depthPlane))
                                }
                                if attachment.resolveTexture == texture {
                                    usedSlices.append((attachment.resolveLevel, attachment.resolveSlice, attachment.resolveDepthPlane))
                                }
                            }
                            
                            for mip in 0..<mipCount {
                                for slice in 0..<arrayLength {
                                    if !usedSlices.contains(where: { $0.level == mip && $0.slice == slice }) {
                                        subresourceRanges.append(VkImageSubresourceRange(aspectMask: pixelFormat.aspectFlags, baseMipLevel: UInt32(mip), levelCount: 1, baseArrayLayer: UInt32(slice), layerCount: 1))
                                    }
                                }
                            }
                        }
                        
                    }
                    
                    // We don't need to insert a barrier; just the render pass layout transitions should be enough.
                    if subresourceRanges.isEmpty { return }
                }
            }
            
            if let buffer = resource.buffer {
                var barrier = VkBufferMemoryBarrier()
                barrier.sType = VK_STRUCTURE_TYPE_BUFFER_MEMORY_BARRIER
                barrier.buffer = resourceMap[buffer].buffer.vkBuffer
                barrier.offset = 0
                barrier.size = VK_WHOLE_SIZE // TODO: track at a more fine-grained level.
                barrier.srcAccessMask = sourceAccessMask
                barrier.dstAccessMask = destinationAccessMask
                barrier.srcQueueFamilyIndex = VK_QUEUE_FAMILY_IGNORED
                barrier.dstQueueFamilyIndex = VK_QUEUE_FAMILY_IGNORED
                bufferBarriers.append(barrier)
            } else if let texture = resource.texture {
                
                var barrier = VkImageMemoryBarrier()
                barrier.sType = VK_STRUCTURE_TYPE_IMAGE_MEMORY_BARRIER
                barrier.image = resourceMap[texture].image.vkImage
                barrier.srcAccessMask = sourceAccessMask
                barrier.dstAccessMask = destinationAccessMask
                barrier.srcQueueFamilyIndex = VK_QUEUE_FAMILY_IGNORED
                barrier.dstQueueFamilyIndex = VK_QUEUE_FAMILY_IGNORED
                barrier.oldLayout = sourceLayout
                barrier.newLayout = destinationLayout
                
                if subresourceRanges.isEmpty {
                    barrier.subresourceRange = VkImageSubresourceRange(aspectMask: texture.descriptor.pixelFormat.aspectFlags, baseMipLevel: 0, levelCount: UInt32(texture.descriptor.mipmapLevelCount), baseArrayLayer: 0, layerCount: UInt32(texture.descriptor.arrayLength))
                    imageBarriers.append(barrier)
                } else {
                    for subresourceRange in subresourceRanges {
                        // Transition the mips/slices not accounted for by the render pass.
                        barrier.subresourceRange = subresourceRange
                        imageBarriers.append(barrier)
                    }
                }
            }
            
            barrierAfterStages.formUnion(sourceMask)
            barrierBeforeStages.formUnion(destinationMask)
            barrierLastIndex = min(beforeCommand, barrierLastIndex)
        }

        for command in commandGenerator.commands {
            if command.index >= barrierLastIndex { // For barriers, the barrier associated with command.index needs to happen _after_ any barriers required to happen _by_ barrierLastIndex
                addBarrier(&compactedResourceCommands)
            }
            
            while !commandInfo.passes[currentPassIndex].commandRange!.contains(command.index) {
                currentPassIndex += 1
            }
            
            while !currentEncoder.commandRange.contains(command.index) {
                currentEncoderIndex += 1
                currentEncoder = commandInfo.commandEncoders[currentEncoderIndex]
                
                assert(bufferBarriers.isEmpty)
                assert(imageBarriers.isEmpty)
            }
            
            // Strategy:
            // useResource should be batched together by usage to as early as possible in the encoder.
            // memoryBarriers should be as late as possible.
            switch command.command {
            case .useResource:
                fatalError("Vulkan does not track resource residency")
            case .memoryBarrier(let resource, let afterUsage, let afterStages, let beforeCommand, let beforeUsage, let beforeStages):
                processMemoryBarrier(resource: resource, afterCommand: command.index, afterUsage: afterUsage, afterStages: afterStages, beforeCommand: beforeCommand, beforeUsage: beforeUsage, beforeStages: beforeStages)
            }
        }
        
        if barrierLastIndex < .max {
            addBarrier(&compactedResourceCommands)
        }
        
        compactedResourceCommands.sort()
    }
}

enum VulkanResourceMemoryBarrier {
    case texture(Texture, VkImageMemoryBarrier)
    case buffer(Buffer, VkBufferMemoryBarrier)
}

struct VulkanMemoryBarrierInfo {
    var sourceMask : VkPipelineStageFlagBits
    var destinationMask : VkPipelineStageFlagBits
    var barrier : VulkanResourceMemoryBarrier
}

enum VulkanCompactedResourceCommandType {
    case signalEvent(VkEvent, afterStages: VkPipelineStageFlagBits)
    
    case waitForEvents(_ events: UnsafeBufferPointer<VkEvent?>, sourceStages: VkPipelineStageFlagBits, destinationStages: VkPipelineStageFlagBits, memoryBarriers: UnsafeBufferPointer<VkMemoryBarrier>, bufferMemoryBarriers: UnsafeBufferPointer<VkBufferMemoryBarrier>, imageMemoryBarriers: UnsafeBufferPointer<VkImageMemoryBarrier>)
    
    case pipelineBarrier(sourceStages: VkPipelineStageFlagBits, destinationStages: VkPipelineStageFlagBits, dependencyFlags: VkDependencyFlagBits, memoryBarriers: UnsafeBufferPointer<VkMemoryBarrier>, bufferMemoryBarriers: UnsafeBufferPointer<VkBufferMemoryBarrier>, imageMemoryBarriers: UnsafeBufferPointer<VkImageMemoryBarrier>)
}


#endif // canImport(Vulkan)
