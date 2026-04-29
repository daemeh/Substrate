//
//  File.swift
//  
//
//  Created by Thomas Roughton on 30/04/22.
//

import Foundation

public final class TaskStream: @unchecked Sendable {
    private let lock = NSLock()
    private let priority: TaskPriority
    private var tail: Task<Void, Never>?

    public init(priority: TaskPriority = .medium) {
        self.priority = priority
    }

    deinit {
        self.lock.lock()
        let tail = self.tail
        self.tail = nil
        self.lock.unlock()
        
        tail?.cancel()
    }

    public func enqueueAndWait<T>(_ perform: @escaping @Sendable () async -> T) async -> T {
        let task: Task<T, Never> = self.lock.withLock {
            let previous = self.tail
            let task = Task.detached(priority: self.priority) {
                await previous?.value
                return await perform()
            }
            self.tail = Task.detached(priority: self.priority) {
                _ = await task.value
            }
            return task
        }
        return await task.value
    }

    public func enqueueAndWait<T>(_ perform: @escaping @Sendable () async throws -> T) async throws -> T {
        let task: Task<T, any Error> = self.lock.withLock {
            let previous = self.tail
            let task = Task.detached(priority: self.priority) {
                await previous?.value
                return try await perform()
            }
            self.tail = Task.detached(priority: self.priority) {
                _ = await task.result
            }
            return task
        }
        return try await task.value
    }

    public func enqueue(_ perform: @escaping @Sendable () async -> Void) {
        self.lock.withLock {
            let previous = self.tail
            self.tail = Task.detached(priority: self.priority) {
                await previous?.value
                await perform()
            }
        }
    }
}
