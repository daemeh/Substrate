//
//  TextureLoading+Texture.swift
//  
//
//  Created by Thomas Roughton on 27/08/20.
//

import Foundation
import Substrate
import stb_image
import SubstrateImage
#if canImport(Metal)
import Metal
#endif

public enum MipGenerationMode {
    /// Generate mipmaps on the CPU using the specified wrap mode and filter
    case cpu(wrapMode: ImageEdgeWrapMode, filter: ImageResizeFilter)
    /// Generate mipmaps using the default GPU mipmap generation method
    case gpuDefault
    /// Skip generating mipmaps, leaving levels below the top-most level uninitialised.
    case skip
}

public protocol TextureCopyable {
    func copyData(to texture: Texture, mipGenerationMode: MipGenerationMode) throws
    var preferredPixelFormat: PixelFormat { get }
}

extension StorageMode {
    public static var preferredForLoadedImage: StorageMode {
        return RenderBackend.hasUnifiedMemory ? .managed : .private
    }
}

public enum TextureCopyError: Error {
    case notTextureCopyable(AnyImage)
}

extension AnyImage {
    public func copyData(to texture: Texture, mipGenerationMode: MipGenerationMode) throws {
        guard let data = self as? TextureCopyable else {
            throw TextureCopyError.notTextureCopyable(self)
        }
        try data.copyData(to: texture, mipGenerationMode: mipGenerationMode)
    }
    
    public var preferredPixelFormat: PixelFormat {
        return (self as? TextureCopyable)?.preferredPixelFormat ?? .invalid
    }
}

extension Image: TextureCopyable {
    public var preferredPixelFormat: PixelFormat {
        let colorSpace = self.colorSpace
        
        switch T.self {
        case is UInt8.Type:
            switch self.channelCount {
            case 1:
                if !RenderBackend.supportsPixelFormat(.r8Unorm_sRGB) { return .r8Unorm }
                return colorSpace == .sRGB ? .r8Unorm_sRGB : .r8Unorm
            case 2:
                if !RenderBackend.supportsPixelFormat(.rg8Unorm_sRGB) { return .rg8Unorm }
                return colorSpace == .sRGB ? .rg8Unorm_sRGB : .rg8Unorm
            case 3, 4:
                return colorSpace == .sRGB ? .rgba8Unorm_sRGB : .rgba8Unorm
            default:
                return .invalid
            }
        case is Int8.Type:
            switch self.channelCount {
            case 1:
                return .r8Snorm
            case 2:
                return .rg8Snorm
            case 3, 4:
                return .rgba8Snorm
            default:
                return .invalid
            }
        case is UInt16.Type:
            switch self.channelCount {
            case 1:
                return .r16Unorm
            case 2:
                return .rg16Unorm
            case 3, 4:
                return .rgba16Unorm
            default:
                return .invalid
            }
        case is Int16.Type:
            switch self.channelCount {
            case 1:
                return .r16Snorm
            case 2:
                return .rg16Snorm
            case 3, 4:
                return .rgba16Snorm
            default:
                return .invalid
            }
        case is UInt32.Type:
            switch self.channelCount {
            case 1:
                return .r32Uint
            case 2:
                return .rg32Uint
            case 3, 4:
                return .rgba32Uint
            default:
                return .invalid
            }
        case is Int32.Type:
            switch self.channelCount {
            case 1:
                return .r32Sint
            case 2:
                return .rg32Sint
            case 3, 4:
                return .rgba32Sint
            default:
                return .invalid
            }
        case is Float.Type:
            switch self.channelCount {
            case 1:
                return .r32Float
            case 2:
                return .rg32Float
            case 3, 4:
                return .rgba32Float
            default:
                return .invalid
            }
        default:
            return .invalid
        }
    }
}

public struct TextureLoadingOptions: OptionSet, Hashable {
    public let rawValue: UInt32
    
    @inlinable
    public init(rawValue: UInt32) {
        self.rawValue = rawValue
    }
    
    /// When the render backend supports RGBA sRGB formats but not e.g. r8Unorm_sRGB, whether to automatically
    /// expand to a four-channel sRGB texture.
    public static let autoExpandSRGBToRGBA = TextureLoadingOptions(rawValue: 1 << 0)
    
    /// Whether to automatically convert the image's color space to match the chosen pixel format's color space.
    public static let autoConvertColorSpace = TextureLoadingOptions(rawValue: 1 << 1)
    
    /// If this option is set, the texture will be converted on load such that blending the loaded texture in linear space will have the same
    /// effect as blending the source texture in gamma space.
    /// For example, when loading an sRGB texture with a color value of 0.0 and an alpha value of 0.5, blending the texture onto a white background will result in an image with a value of 0.5 in sRGB space rather than a value of 0.5 in linear space.
    public static let assumeSourceImageUsesGammaSpaceBlending = TextureLoadingOptions(rawValue: 1 << 2)
    
    /// When loading a texture with an undefined color space, treat that texture's contents as being sRGB.
    public static let mapUndefinedColorSpaceToSRGB = TextureLoadingOptions(rawValue: 1 << 3)
    
    public static let `default`: TextureLoadingOptions = [.autoConvertColorSpace, .autoExpandSRGBToRGBA]
}

public enum TextureLoadingError : Error {
    case noSupportedPixelFormat
    case mismatchingPixelFormat(expected: PixelFormat, actual: PixelFormat)
    case mismatchingDimensions(expected: Size, actual: Size)
}

extension Image {
    public func copyData(to texture: Texture, region: Region, mipmapLevel: Int, slice: Int = 0) {
        if texture.descriptor.storageMode == .private {
#if canImport(Metal)
            if case .vm_allocate = self.allocator {
                // On Metal, we can make vm_allocate'd buffers directly accessible to the GPU.
                let allocatedSize = self.allocatedSize
                let success = self.withUnsafeBufferPointer { bytes -> Bool in
                    guard let mtlBuffer = (RenderBackend.renderDevice as! MTLDevice).makeBuffer(bytesNoCopy: UnsafeMutableRawPointer(mutating: bytes.baseAddress!), length: allocatedSize, options: .storageModeShared, deallocator: nil) else { return false }
                    let substrateBuffer = Buffer(descriptor: BufferDescriptor(length: allocatedSize, storageMode: .shared, cacheMode: .defaultCache, usage: .blitSource), externalResource: mtlBuffer)
                    GPUResourceUploader.runBlitPass { bce in
                        bce.copy(from: substrateBuffer, sourceOffset: 0, sourceBytesPerRow: self.width * self.channelCount * MemoryLayout<ComponentType>.stride, sourceBytesPerImage: self.width * self.height * self.channelCount * MemoryLayout<ComponentType>.stride, sourceSize: region.size, to: texture, destinationSlice: slice, destinationLevel: mipmapLevel, destinationOrigin: Origin())
                    }
                    substrateBuffer.dispose()
                    return true
                }
                if success {
                    return
                }
            }
#endif
            if case .custom(let context, _) = self.allocator,
               let uploadBufferToken = context as? GPUResourceUploader.UploadBufferToken {
                let buffer = uploadBufferToken.stagingBuffer!
                let sourceOffset = self.withUnsafeBufferPointer { bytes in
                    buffer.withContents { return UnsafeRawPointer(bytes.baseAddress!) - $0.baseAddress! }
                }
                uploadBufferToken.didModifyBuffer()
                
                let executionToken = GPUResourceUploader.runBlitPass { bce in
                    bce.copy(from: buffer, sourceOffset: sourceOffset, sourceBytesPerRow: self.width * self.channelCount * MemoryLayout<T>.stride, sourceBytesPerImage: self.width * self.height * self.channelCount * MemoryLayout<T>.stride, sourceSize: region.size, to: texture, destinationSlice: slice, destinationLevel: mipmapLevel, destinationOrigin: Origin())
                }
                uploadBufferToken.didFlush(token: executionToken)
                return
            }
        }
        
        self.withUnsafeBufferPointer { bytes in
            _ = GPUResourceUploader.replaceTextureRegion(region, mipmapLevel: mipmapLevel, in: texture, withBytes: bytes.baseAddress!, bytesPerRow: self.width * self.channelCount * MemoryLayout<T>.stride)
        }
    }
    
    public func copyData(to texture: Texture, mipGenerationMode: MipGenerationMode = .gpuDefault) throws {
        if _isDebugAssertConfiguration(), self.colorSpace == .sRGB, !texture.descriptor.pixelFormat.isSRGB {
            print("Warning: the source texture data is in the sRGB color space but the texture's pixel format is linear RGB.")
        }
        
        guard self.preferredPixelFormat.channelCount == texture.descriptor.pixelFormat.channelCount, self.preferredPixelFormat.bytesPerPixel == texture.descriptor.pixelFormat.bytesPerPixel else {
            throw TextureLoadingError.mismatchingPixelFormat(expected: self.preferredPixelFormat, actual: texture.descriptor.pixelFormat)
        }
        guard texture.descriptor.width == self.width, texture.descriptor.height == self.height else {
            throw TextureLoadingError.mismatchingDimensions(expected: Size(width: self.width, height: self.height), actual: texture.descriptor.size)
        }
        
        if texture.descriptor.mipmapLevelCount > 1, case .cpu(let wrapMode, let filter) = mipGenerationMode {
            let mips = self.generateMipChain(wrapMode: wrapMode, filter: filter, compressedBlockSize: 1, mipmapCount: texture.descriptor.mipmapLevelCount)
                           
            for (i, data) in mips.enumerated().prefix(texture.descriptor.mipmapLevelCount) {
                data.copyData(to: texture, region: Region(x: 0, y: 0, width: data.width, height: data.height), mipmapLevel: i)
            }
        } else {
            self.copyData(to: texture, region: Region(x: 0, y: 0, width: self.width, height: self.height), mipmapLevel: 0)
            if texture.descriptor.mipmapLevelCount > 1, case .gpuDefault = mipGenerationMode {
                if _isDebugAssertConfiguration(), self.channelCount == 4, self.alphaMode == .postmultiplied {
                    print("Warning: generating mipmaps using the GPU's default mipmap generation for texture \(texture.label ?? "Texture(handle: \(texture.handle))") which expects premultiplied alpha, but the texture has an alpha mode of \(self.alphaMode). Fringing may be visible")
                }
                GPUResourceUploader.generateMipmaps(for: texture)
            }
        }
    }
}

public struct DirectToTextureImageLoadingDelegate: ImageLoadingDelegate {
    let storageMode: StorageMode
    let options: TextureLoadingOptions
    
    public init(storageMode: StorageMode = .managed, options: TextureLoadingOptions = .default) {
        self.storageMode = storageMode
        self.options = options
    }
    
    public func channelCount(for imageInfo: ImageFileInfo) -> Int {
        let isSRGB = imageInfo.colorSpace == .sRGB || (imageInfo.colorSpace == .undefined && options.contains(.mapUndefinedColorSpaceToSRGB))
        if (options.contains(.autoExpandSRGBToRGBA) && isSRGB && imageInfo.channelCount < 4) || imageInfo.channelCount == 3 {
            var needsChannelExpansion = true
            if (imageInfo.channelCount == 1 && RenderBackend.supportsPixelFormat(.r8Unorm_sRGB)) ||
                (imageInfo.channelCount == 2 && RenderBackend.supportsPixelFormat(.rg8Unorm_sRGB)) {
                needsChannelExpansion = false
            }
            return needsChannelExpansion ? 4 : imageInfo.channelCount
        }
        return imageInfo.channelCount
    }
    
    public func allocateMemory(byteCount: Int, alignment: Int, zeroed: Bool) throws -> (allocation: UnsafeMutableRawBufferPointer, allocator: ImageAllocator) {
        if self.storageMode != .private {
            // We're going to use copyBytes rather than blitting from GPU-accessible memory, so we can allocate using the default allocator.
            return ImageAllocator.allocateMemoryDefault(byteCount: byteCount, alignment: alignment, zeroed: zeroed)
        }
        
        let uploadBufferToken = GPUResourceUploader.extendedLifetimeUploadBuffer(length: byteCount, alignment: alignment, cacheMode: .defaultCache)
        if zeroed {
            _ = uploadBufferToken.contents.initializeMemory(as: UInt8.self, repeating: 0)
        }
        return (uploadBufferToken.contents, .custom(context: uploadBufferToken, deallocateFunc: { _, context in
            (context as! GPUResourceUploader.UploadBufferToken).flush()
        }))
    }
}

extension Texture {
    public init(fileAt url: URL, colorSpace: ImageColorSpace = .undefined, sourceAlphaMode: ImageAlphaMode = .inferred, gpuAlphaMode: ImageAlphaMode = .none, mipmapped: Bool, mipGenerationMode: MipGenerationMode = .gpuDefault, storageMode: StorageMode = .preferredForLoadedImage, usage: TextureUsage = .shaderRead, options: TextureLoadingOptions = .default) throws {
        let usage = usage.union(storageMode == .private ? TextureUsage.blitDestination : [])
        
        self = Texture._createPersistentTextureWithoutDescriptor(flags: .persistent)
        try self.fillInternal(fromFileAt: url, colorSpace: colorSpace, sourceAlphaMode: sourceAlphaMode, gpuAlphaMode: gpuAlphaMode, mipmapped: mipmapped, mipGenerationMode: mipGenerationMode, storageMode: storageMode, usage: usage, options: options, isPartiallyInitialised: true)
    }
    
    public init(decodingImageData imageData: Data, colorSpace: ImageColorSpace = .undefined, sourceAlphaMode: ImageAlphaMode = .inferred, gpuAlphaMode: ImageAlphaMode = .none, mipmapped: Bool, mipGenerationMode: MipGenerationMode = .gpuDefault, storageMode: StorageMode = .preferredForLoadedImage, usage: TextureUsage = .shaderRead, options: TextureLoadingOptions = .default) throws {
        let usage = usage.union(storageMode == .private ? TextureUsage.blitDestination : [])
        
        self = Texture._createPersistentTextureWithoutDescriptor(flags: .persistent)
        try self.fillInternal(imageData: imageData, colorSpace: colorSpace, sourceAlphaMode: sourceAlphaMode, gpuAlphaMode: gpuAlphaMode, mipmapped: mipmapped, mipGenerationMode: mipGenerationMode, storageMode: storageMode, usage: usage, options: options, isPartiallyInitialised: true)
    }
    
    @available(*, deprecated, renamed: "init(fileAt:mipmapped:colorSpace:alphaMode:storageMode:usage:)")
    public init(fileAt url: URL, mipmapped: Bool, colorSpace: ImageColorSpace, premultipliedAlpha: Bool, storageMode: StorageMode = .preferredForLoadedImage, usage: TextureUsage = .shaderRead) throws {
        try self.init(fileAt: url, colorSpace: colorSpace, sourceAlphaMode: premultipliedAlpha ? .premultiplied : .postmultiplied, mipmapped: mipmapped, storageMode: storageMode, usage: usage)
    }
    
    @available(*, deprecated, renamed: "init(fileAt:mipmapped:colorSpace:alphaMode:storageMode:usage:)")
    public init(fileAt url: URL, mipmapped: Bool, colourSpace: ImageColorSpace, premultipliedAlpha: Bool, storageMode: StorageMode = .preferredForLoadedImage, usage: TextureUsage = .shaderRead) throws {
        try self.init(fileAt: url, colorSpace: colourSpace, sourceAlphaMode: premultipliedAlpha ? .premultiplied : .postmultiplied, mipmapped: mipmapped, storageMode: storageMode, usage: usage)
    }
    
    public func copyData(from textureData: AnyImage, mipGenerationMode: MipGenerationMode = .gpuDefault) throws {
        return try textureData.copyData(to: self, mipGenerationMode: mipGenerationMode)
    }

    private static func processSourceImage(_ textureData: inout Image<UInt8>, gpuAlphaMode: ImageAlphaMode, options: TextureLoadingOptions) throws {
        if options.contains(.mapUndefinedColorSpaceToSRGB), textureData.colorSpace == .undefined {
            textureData.reinterpretColor(as: .sRGB)
        }
        
        if textureData.alphaMode != .none {
            if options.contains(.assumeSourceImageUsesGammaSpaceBlending), textureData.colorSpace == .sRGB {
                textureData.convertToPostmultipliedAlpha()
                textureData.convertPostmultSRGBBlendedSRGBToPremultLinearBlendedSRGB()
            }
            
            switch gpuAlphaMode {
            case .premultiplied:
                textureData.convertToPremultipliedAlpha()
            case .postmultiplied:
                textureData.convertToPostmultipliedAlpha()
            default:
                break
            }
        }
        
        let pixelFormat = textureData.preferredPixelFormat
        guard pixelFormat != .invalid else {
            throw TextureLoadingError.noSupportedPixelFormat
        }
        
        if options.contains(.autoConvertColorSpace) {
            if pixelFormat.isSRGB, textureData.colorSpace != .sRGB {
                textureData.convert(toColorSpace: .sRGB)
            } else if !pixelFormat.isSRGB {
                textureData.convert(toColorSpace: .linearSRGB)
            }
        }
    }
    
    private static func processSourceImage(_ textureData: inout Image<UInt16>, gpuAlphaMode: ImageAlphaMode, options: TextureLoadingOptions) throws {
        if options.contains(.mapUndefinedColorSpaceToSRGB), textureData.colorSpace == .undefined {
            textureData.reinterpretColor(as: .sRGB)
        }
        
        let pixelFormat = textureData.preferredPixelFormat
        guard pixelFormat != .invalid else {
            throw TextureLoadingError.noSupportedPixelFormat
        }
        
        if textureData.alphaMode != .none {
            switch gpuAlphaMode {
            case .premultiplied:
                textureData.convertToPremultipliedAlpha()
            case .postmultiplied:
                textureData.convertToPostmultipliedAlpha()
            default:
                break
            }
        }
        
        if options.contains(.autoConvertColorSpace) {
            if pixelFormat.isSRGB, textureData.colorSpace != .sRGB {
                textureData.convert(toColorSpace: .sRGB)
            } else if !pixelFormat.isSRGB {
                textureData.convert(toColorSpace: .linearSRGB)
            }
        }
    }
    
    private static func processSourceImage(_ textureData: inout Image<Float>, colorSpace: ImageColorSpace, gpuAlphaMode: ImageAlphaMode, options: TextureLoadingOptions) throws {
        if colorSpace != .undefined {
            textureData.reinterpretColor(as: colorSpace)
        } else if options.contains(.mapUndefinedColorSpaceToSRGB), textureData.colorSpace == .undefined {
            textureData.reinterpretColor(as: .sRGB)
        }
        
        let pixelFormat = textureData.preferredPixelFormat
        guard pixelFormat != .invalid else {
            throw TextureLoadingError.noSupportedPixelFormat
        }
        
        if options.contains(.autoConvertColorSpace) {
            if pixelFormat.isSRGB, textureData.colorSpace != .sRGB {
                textureData.convert(toColorSpace: .sRGB)
            } else if !pixelFormat.isSRGB {
                textureData.convert(toColorSpace: .linearSRGB)
            }
        }
        
        if textureData.alphaMode != .none {
            switch gpuAlphaMode {
            case .premultiplied:
                textureData.convertToPremultipliedAlpha()
            case .postmultiplied:
                textureData.convertToPostmultipliedAlpha()
            default:
                break
            }
        }
    }
    
    public static func loadSourceImage(fromFileAt url: URL, colorSpace: ImageColorSpace, sourceAlphaMode: ImageAlphaMode, gpuAlphaMode: ImageAlphaMode, options: TextureLoadingOptions, loadingDelegate: ImageLoadingDelegate? = nil) throws -> AnyImage {
        if url.pathExtension.lowercased() == "exr" {
            var textureData = try Image<Float>(exrAt: url, loadingDelegate: loadingDelegate)
            try self.processSourceImage(&textureData, colorSpace: colorSpace, gpuAlphaMode: gpuAlphaMode, options: options)
            return textureData
        }
        
        let fileInfo = try ImageFileInfo(url: url)
        
        let is16Bit = fileInfo.bitDepth == 16
        let isHDR = fileInfo.isFloatingPoint
        
        if isHDR {
            var textureData = try Image<Float>(fileAt: url, colorSpace: colorSpace, alphaMode: sourceAlphaMode, loadingDelegate: loadingDelegate)
            try self.processSourceImage(&textureData, colorSpace: colorSpace, gpuAlphaMode: gpuAlphaMode, options: options)
            return textureData
            
        } else if is16Bit {
            var textureData = try Image<UInt16>(fileAt: url, colorSpace: colorSpace, alphaMode: sourceAlphaMode, loadingDelegate: loadingDelegate)
            try self.processSourceImage(&textureData, gpuAlphaMode: gpuAlphaMode, options: options)
            return textureData
            
        } else {
            var textureData = try Image<UInt8>(fileAt: url, colorSpace: colorSpace, alphaMode: sourceAlphaMode, loadingDelegate: loadingDelegate)
            try self.processSourceImage(&textureData, gpuAlphaMode: gpuAlphaMode, options: options)
            return textureData
        }
    }
    
    public static func loadSourceImage(decodingImageData imageData: Data, colorSpace: ImageColorSpace, sourceAlphaMode: ImageAlphaMode, gpuAlphaMode: ImageAlphaMode, options: TextureLoadingOptions, loadingDelegate: ImageLoadingDelegate? = nil) throws -> AnyImage {
        
        let fileInfo = try ImageFileInfo(data: imageData)
        if fileInfo.format == .exr {
            var textureData = try Image<Float>(exrData: imageData, loadingDelegate: loadingDelegate)
            try self.processSourceImage(&textureData, colorSpace: colorSpace, gpuAlphaMode: gpuAlphaMode, options: options)
            return textureData
        }
        
        let is16Bit = fileInfo.bitDepth == 16
        let isHDR = fileInfo.isFloatingPoint
        
        if isHDR {
            var textureData = try Image<Float>(data: imageData, colorSpace: colorSpace, alphaMode: sourceAlphaMode, loadingDelegate: loadingDelegate)
            try self.processSourceImage(&textureData, colorSpace: colorSpace, gpuAlphaMode: gpuAlphaMode, options: options)
            return textureData
            
        } else if is16Bit {
            var textureData = try Image<UInt16>(data: imageData, colorSpace: colorSpace, alphaMode: sourceAlphaMode, loadingDelegate: loadingDelegate)
            try self.processSourceImage(&textureData, gpuAlphaMode: gpuAlphaMode, options: options)
            return textureData
            
        } else {
            var textureData = try Image<UInt8>(data: imageData, colorSpace: colorSpace, alphaMode: sourceAlphaMode, loadingDelegate: loadingDelegate)
            try self.processSourceImage(&textureData, gpuAlphaMode: gpuAlphaMode, options: options)
            return textureData
        }
    }

    private func fillInternal(fromFileAt url: URL, colorSpace: ImageColorSpace, sourceAlphaMode: ImageAlphaMode, gpuAlphaMode: ImageAlphaMode, mipmapped: Bool, mipGenerationMode: MipGenerationMode, storageMode: StorageMode, usage: TextureUsage, options: TextureLoadingOptions, isPartiallyInitialised: Bool) throws {
        precondition(storageMode != .private || usage.contains(.blitDestination))
        
        let textureData = try Texture.loadSourceImage(fromFileAt: url, colorSpace: colorSpace, sourceAlphaMode: sourceAlphaMode, gpuAlphaMode: gpuAlphaMode, options: options, loadingDelegate: DirectToTextureImageLoadingDelegate(storageMode: storageMode, options: options))
        
        if isPartiallyInitialised {
            let descriptor = TextureDescriptor(type: .type2D, format: textureData.preferredPixelFormat, width: textureData.width, height: textureData.height, mipmapped: mipmapped, storageMode: storageMode, usage: usage)
            self._initialisePersistentTexture(descriptor: descriptor, heap: nil)
        }
        
        try textureData.copyData(to: self, mipGenerationMode: mipGenerationMode)
        
        if self.label == nil {
            self.label = url.lastPathComponent
        }
    }
    
    private func fillInternal(imageData: Data, colorSpace: ImageColorSpace, sourceAlphaMode: ImageAlphaMode, gpuAlphaMode: ImageAlphaMode, mipmapped: Bool, mipGenerationMode: MipGenerationMode, storageMode: StorageMode, usage: TextureUsage, options: TextureLoadingOptions, isPartiallyInitialised: Bool) throws {
        precondition(storageMode != .private || usage.contains(.blitDestination))
        
        let textureData = try Texture.loadSourceImage(decodingImageData: imageData, colorSpace: colorSpace, sourceAlphaMode: sourceAlphaMode, gpuAlphaMode: gpuAlphaMode, options: options, loadingDelegate: DirectToTextureImageLoadingDelegate(storageMode: storageMode, options: options))
        
        if isPartiallyInitialised {
            let descriptor = TextureDescriptor(type: .type2D, format: textureData.preferredPixelFormat, width: textureData.width, height: textureData.height, mipmapped: mipmapped, storageMode: storageMode, usage: usage)
            self._initialisePersistentTexture(descriptor: descriptor, heap: nil)
        }
        
        try textureData.copyData(to: self, mipGenerationMode: mipGenerationMode)
    }
    
    public func fill(fromFileAt url: URL, colorSpace: ImageColorSpace = .undefined, sourceAlphaMode: ImageAlphaMode = .inferred, gpuAlphaMode: ImageAlphaMode = .none, mipGenerationMode: MipGenerationMode = .gpuDefault, options: TextureLoadingOptions = .default) throws {
        try self.fillInternal(fromFileAt: url, colorSpace: colorSpace, sourceAlphaMode: sourceAlphaMode, gpuAlphaMode: gpuAlphaMode, mipmapped: self.descriptor.mipmapLevelCount > 1, mipGenerationMode: mipGenerationMode, storageMode: self.descriptor.storageMode, usage: self.descriptor.usageHint, options: options, isPartiallyInitialised: false)
    }
    
    public func fill(decodingImageData imageData: Data, colorSpace: ImageColorSpace = .undefined, sourceAlphaMode: ImageAlphaMode = .inferred, gpuAlphaMode: ImageAlphaMode = .none, mipGenerationMode: MipGenerationMode = .gpuDefault, options: TextureLoadingOptions = .default) throws {
        try self.fillInternal(imageData: imageData, colorSpace: colorSpace, sourceAlphaMode: sourceAlphaMode, gpuAlphaMode: gpuAlphaMode, mipmapped: self.descriptor.mipmapLevelCount > 1, mipGenerationMode: mipGenerationMode, storageMode: self.descriptor.storageMode, usage: self.descriptor.usageHint, options: options, isPartiallyInitialised: false)
    }
}
