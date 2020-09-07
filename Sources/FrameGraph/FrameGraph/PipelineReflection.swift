//
//  PipelineReflection.swift
//  RenderAPI
//
//  Created by Joseph Bennett on 20/12/17.
//

import FrameGraphUtilities

public enum ResourceUsageType {
    case read
    case write
    case readWrite

    /// A render target attachment that is unused.
    case unusedRenderTarget
    /// A render target that is only written to (e.g. a color attachment with blending disabled)
    case writeOnlyRenderTarget
    /// A render target that is also read from, whether by blending or by depth/stencil operations
    case readWriteRenderTarget
    /// A render target that is simultaneously used as an input attachment (including read or sample operations).
    case inputAttachmentRenderTarget

    case sampler
    case inputAttachment
    case constantBuffer

    case blitSource
    case blitDestination
    case blitSynchronisation
    case mipGeneration
    
    case vertexBuffer
    case indexBuffer
    case indirectBuffer
    
    // Present in an argument buffer, but not actually used until later on.
    case unusedArgumentBuffer

    // Used in a previous frame
    case previousFrame

    public var isRenderTarget : Bool {
        switch self {
        case .unusedRenderTarget, .writeOnlyRenderTarget, .readWriteRenderTarget, .inputAttachmentRenderTarget:
            return true
        default:
            return false
        }
    }
}

/// A generic resource binding path.
/// Can be customised by the backends to any size-compatible POD type,
/// and then converted into a ResourceBindingPath for use of the FrameGraph.
public struct ResourceBindingPath : Hashable {
    public var value : UInt64
    
    public init(value: UInt64) {
        self.value = value
    }
    
    public static let `nil` = ResourceBindingPath(value: UInt64.max)
    
    @inlinable
    public static func ==(lhs: ResourceBindingPath, rhs: ResourceBindingPath) -> Bool {
        return lhs.value == rhs.value
    }
}

extension ResourceBindingPath : CustomHashable {
    public var customHashValue : Int {
        return Int(truncatingIfNeeded: self.value &* 39)
    }
}

public enum ActiveResourceRange {
    case inactive
    case fullResource
    case buffer(Range<Int>)
    case texture(TextureSubresourceMask)
    
    @inlinable
    init(_ range: ActiveResourceRange, resource: Resource, allocator: AllocatorType) {
        switch range {
        case .texture(let textureMask):
            self = .texture(TextureSubresourceMask(source: textureMask, descriptor: resource.texture!.descriptor, allocator: allocator))
        default:
            self = range
        }
    }
    
    @inlinable
    mutating func formUnion(with range: ActiveResourceRange, resource: Resource, allocator: AllocatorType) {
        if case .fullResource = self {
            return
        }
        switch (self, range) {
        case (.inactive, _):
            self = range
        case (_, .inactive):
            break
        case (.fullResource, _),
             (_, .fullResource):
            self = .fullResource
        case (.buffer(let rangeA), .buffer(let rangeB)):
            self = .buffer(min(rangeA.lowerBound, rangeB.lowerBound)..<max(rangeA.upperBound, rangeB.upperBound))
        case (.texture(var maskA), .texture(let maskB)):
            maskA.formUnion(with: maskB, descriptor: resource.texture!.descriptor, allocator: allocator)
            self = .texture(maskA)
        default:
            fatalError("Incompatible resource ranges \(self) and \(range)")
        }
    }
    
    func union(with range: ActiveResourceRange, resource: Resource, allocator: AllocatorType) -> ActiveResourceRange {
        var result = ActiveResourceRange(self, resource: resource, allocator: allocator)
        result.formIntersection(with: range, resource: resource, allocator: allocator)
        return result
    }
    
    @inlinable
    mutating func formIntersection(with range: ActiveResourceRange, resource: Resource, allocator: AllocatorType) {
        if case .fullResource = self {
            self = ActiveResourceRange(range, resource: resource, allocator: allocator)
        }
        switch (self, range) {
        case (.inactive, _), (_, .inactive):
            self = .inactive
        case (.fullResource, _):
            self = ActiveResourceRange(range, resource: resource, allocator: allocator)
        case (_, .fullResource):
            return
        case (.buffer(let rangeA), .buffer(let rangeB)):
            self = rangeA.overlaps(rangeB) ? .buffer(max(rangeA.lowerBound, rangeB.lowerBound)..<min(rangeA.upperBound, rangeB.upperBound)) : .inactive
        case (.texture(var maskA), .texture(let maskB)):
            maskA.formIntersection(with: maskB, descriptor: resource.texture!.descriptor, allocator: allocator)
            self = .texture(maskA)
        default:
            fatalError("Incompatible resource ranges \(self) and \(range)")
        }
    }
    
    func intersection(with range: ActiveResourceRange, resource: Resource, allocator: AllocatorType) -> ActiveResourceRange {
        var result = ActiveResourceRange(self, resource: resource, allocator: allocator)
        result.formIntersection(with: range, resource: resource, allocator: allocator)
        return result
    }
    
    @inlinable
    mutating func subtract(range: ActiveResourceRange, resource: Resource, allocator: AllocatorType) {
        switch (self, range) {
        case (.inactive, _):
            self = .inactive
        case (_, .inactive):
            return
        case (_, .fullResource):
            self = .inactive
        case (.fullResource, .texture(let textureRange)):
            var result = TextureSubresourceMask()
            result.removeAll(in: textureRange, descriptor: resource.texture!.descriptor, allocator: allocator)
            self = .texture(result)
        case (.buffer, .buffer),
             (.fullResource, .buffer):
            fatalError("Subtraction for buffer ranges is not implemented; we really need a RangeSet type (or a TextureSubresourceMask-like coarse tracking for the buffer) to handle this properly.")
        case (.texture(var maskA), .texture(let maskB)):
            maskA.removeAll(in: maskB, descriptor: resource.texture!.descriptor, allocator: allocator)
            self = .texture(maskA)
        default:
            fatalError("Incompatible resource ranges \(self) and \(range)")
        }
    }
    
    
    @inlinable
    func subtracting(range: ActiveResourceRange, resource: Resource, allocator: AllocatorType) -> ActiveResourceRange {
        var result = ActiveResourceRange(self, resource: resource, allocator: allocator)
        result.subtract(range: range, resource: resource, allocator: allocator)
        return result
    }
    
    public func isEqual(to other: ActiveResourceRange, resource: Resource) -> Bool {
        switch (self, other) {
        case (.inactive, .inactive):
            return true
        case (.fullResource, .fullResource):
            return true
        case (.inactive, .fullResource), (.fullResource, .inactive):
            return false
        case (.buffer(let rangeA), .buffer(let rangeB)):
            return rangeA == rangeB
        case (.texture(let maskA), .texture(let maskB)):
            return maskA.isEqual(to: maskB, descriptor: resource.texture!.descriptor)
            
        case (.buffer(let range), .fullResource), (.fullResource, .buffer(let range)):
            return range == resource.buffer!.range
        case (.buffer(let range), .inactive), (.inactive, .buffer(let range)):
            return range.isEmpty
            
        case (.texture(let mask), .fullResource), (.fullResource, .texture(let mask)):
            return mask.value == .max
        case (.texture(let mask), .inactive), (.inactive, .texture(let mask)):
            return mask.value == 0
            
        default:
            fatalError("Incompatible resource ranges \(self) and \(other)")
        }
    }
    
    func offset(by offset: Int) -> ActiveResourceRange {
        if case .buffer(let range) = self {
            return .buffer((range.lowerBound + offset)..<(range.upperBound + offset))
        }
        return self
    }
    
    
}

public struct ArgumentReflection {
    public var type : ResourceType
    public var bindingPath : ResourceBindingPath
    public var usageType : ResourceUsageType
    public var activeStages : RenderStages
    public var activeRange: ActiveResourceRange
    
    public init(type: ResourceType, bindingPath: ResourceBindingPath, usageType: ResourceUsageType, activeStages: RenderStages, activeRange: ActiveResourceRange) {
        self.type = type
        self.bindingPath = bindingPath
        self.usageType = usageType
        self.activeStages = activeStages
        self.activeRange = activeRange
    }
    
    public var isActive: Bool {
        return !self.activeStages.isEmpty
    }
}
