//
//  PixelFormat.swift
//  CGRA 402
//
//  Created by Thomas Roughton on 10/03/17.
//  Copyright © 2017 Thomas Roughton. All rights reserved.
//

import Swift

public enum BlendFactor : UInt, Hashable, Codable {
    
    case zero
    
    case one
    
    case sourceColor
    
    case oneMinusSourceColor
    
    case sourceAlpha
    
    case oneMinusSourceAlpha
    
    case destinationColor
    
    case oneMinusDestinationColor
    
    case destinationAlpha
    
    case oneMinusDestinationAlpha
    
    case sourceAlphaSaturated
    
    case blendColor
    
    case oneMinusBlendColor
    
    case blendAlpha
    
    case oneMinusBlendAlpha
    
    case source1Color
    
    case oneMinusSource1Color
    
    case source1Alpha
    
    case oneMinusSource1Alpha
}

public enum BlendOperation : UInt, Hashable, Codable {
    
    case add
    
    case subtract
    
    case reverseSubtract
    
    case min
    
    case max
}

public struct ColorWriteMask : OptionSet, Hashable, Codable {
    
    public var rawValue: UInt
    
    @inlinable
    public init(rawValue: UInt) {
        self.rawValue = rawValue
    }
    
    public static let red = ColorWriteMask(rawValue: 1)
    
    public static let green = ColorWriteMask(rawValue: 2)
    
    public static let blue = ColorWriteMask(rawValue: 4)
    
    public static let alpha = ColorWriteMask(rawValue: 8)
    
    public static let all : ColorWriteMask = [.red, .green, .blue, .alpha]
}


public enum PixelFormat : UInt, Hashable, Codable, CaseIterable {
    
    case invalid = 0
    
    /* Normal 8 bit formats */
    case a8Unorm = 1
    
    case r8Unorm = 10
    case r8Unorm_sRGB = 11
    
    case r8Snorm = 12
    case r8Uint = 13
    case r8Sint = 14
    
    /* Normal 16 bit formats */
    case r16Unorm = 20
    case r16Snorm = 22
    case r16Uint = 23
    case r16Sint = 24
    case r16Float = 25
    
    case rg8Unorm = 30
    case rg8Unorm_sRGB = 31
    case rg8Snorm = 32
    case rg8Uint = 33
    case rg8Sint = 34
    
    case b5g6r5Unorm = 40
    case a1bgr5Unorm = 41
    case abgr4Unorm = 42
    case bgr5a1Unorm = 43
    
    /* Normal 32 bit formats */
    case r32Uint = 53
    case r32Sint = 54
    case r32Float = 55
    
    case rg16Unorm = 60
    case rg16Snorm = 62
    case rg16Uint = 63
    case rg16Sint = 64
    case rg16Float = 65
    
    case rgba8Unorm = 70
    case rgba8Unorm_sRGB = 71
    case rgba8Snorm = 72
    case rgba8Uint = 73
    case rgba8Sint = 74
    
    case bgra8Unorm = 80
    case bgra8Unorm_sRGB = 81
    
    /* Packed 32 bit formats */
    case rgb10a2Unorm = 90
    case rgb10a2Uint = 91
    
    case rg11b10Float = 92
    case rgb9e5Float = 93
    case bgr10a2Unorm = 94
    
    case bgr10_xr = 554
    case bgr10_xr_sRGB = 555
    
    /* Normal 64 bit formats */
    case rg32Uint = 103
    case rg32Sint = 104
    case rg32Float = 105
    
    case rgba16Unorm = 110
    case rgba16Snorm = 112
    case rgba16Uint = 113
    case rgba16Sint = 114
    case rgba16Float = 115
    
    case bgra10_xr = 552
    case bgra10_xr_sRGB = 553
    
    /* Normal 128 bit formats */
    case rgba32Uint = 123
    case rgba32Sint = 124
    case rgba32Float = 125
    
    /* Compressed formats. */
    /* S3TC/DXT */
    case bc1_rgba = 130
    case bc1_rgba_sRGB = 131
    case bc2_rgba = 132
    case bc2_rgba_sRGB = 133
    case bc3_rgba = 134
    case bc3_rgba_sRGB = 135
    
    /* RGTC */
    case bc4_rUnorm = 140
    case bc4_rSnorm = 141
    case bc5_rgUnorm = 142
    case bc5_rgSnorm = 143
    
    /* BPTC */
    case bc6H_rgbFloat = 150
    case bc6H_rgbuFloat = 151
    case bc7_rgbaUnorm = 152
    case bc7_rgbaUnorm_sRGB = 153
    

    case gbgr422 = 240
    
    case bgrg422 = 241
    
    /* Depth */

    case depth16Unorm = 250
    case depth32Float = 252
    
    /* Stencil */

    case stencil8 = 253
    
    /* Depth Stencil */

    case depth24Unorm_stencil8 = 255
    case depth32Float_stencil8 = 260

    case x32_stencil8 = 261
    case x24_stencil8 = 262
    
    @inlinable
    public var isSRGB : Bool {
        switch self {
        case .r8Unorm_sRGB, .rg8Unorm_sRGB, .rgba8Unorm_sRGB, .bgra8Unorm_sRGB, .bgr10_xr_sRGB, .bgra10_xr_sRGB, .bc1_rgba_sRGB, .bc2_rgba_sRGB, .bc3_rgba_sRGB, .bc7_rgbaUnorm_sRGB:
            return true
        default:
            return false
        }
    }
    
    @inlinable
    public var channelCount : Int {
        switch self {
        case .invalid:
            return 0
            
        case .r8Sint, .r8Uint, .r8Snorm, .r8Unorm, .r8Unorm_sRGB, .a8Unorm,
             .r16Sint, .r16Uint, .r16Float, .r16Snorm, .r16Unorm,
             .r32Sint, .r32Uint, .r32Float,
             .depth16Unorm, .depth32Float, .stencil8,
             .bc4_rSnorm, .bc4_rUnorm:
            return 1
            
        case .rg8Sint, .rg8Uint, .rg8Snorm, .rg8Unorm, .rg8Unorm_sRGB,
             .rg16Sint, .rg16Uint, .rg16Float, .rg16Snorm, .rg16Unorm,
             .rg32Sint, .rg32Uint, .rg32Float,
             .depth24Unorm_stencil8, .x24_stencil8, .depth32Float_stencil8, .x32_stencil8,
             .bc5_rgSnorm, .bc5_rgUnorm:
            return 2
            
        case .b5g6r5Unorm, .rg11b10Float, .rgb9e5Float, .bgr10_xr, .bgr10_xr_sRGB,
             .bc6H_rgbFloat, .bc6H_rgbuFloat,
             .gbgr422, .bgrg422:
            return 3
            
        case .rgba8Sint, .rgba8Uint, .rgba8Snorm, .rgba8Unorm, .rgba8Unorm_sRGB,
             .bgra8Unorm, .bgra8Unorm_sRGB,
             .rgba16Sint, .rgba16Uint, .rgba16Float, .rgba16Snorm, .rgba16Unorm,
             .rgba32Sint, .rgba32Uint, .rgba32Float,
             .a1bgr5Unorm, .abgr4Unorm, .bgr5a1Unorm,
             .rgb10a2Uint, .rgb10a2Unorm, .bgr10a2Unorm,
             .bgra10_xr, .bgra10_xr_sRGB,
             .bc1_rgba, .bc1_rgba_sRGB,
             .bc2_rgba, .bc2_rgba_sRGB,
             .bc3_rgba, .bc3_rgba_sRGB,
             .bc7_rgbaUnorm, .bc7_rgbaUnorm_sRGB:
            return 4
        }
    }
    
    @inlinable
    public var bytesPerPixel : Double {
        switch self {
        case .invalid:
            return 0
            
        // 8 bits per channel
        case .r8Sint, .r8Uint, .r8Snorm, .r8Unorm, .r8Unorm_sRGB, .a8Unorm:
            return 1
        case .rg8Sint, .rg8Uint, .rg8Snorm, .rg8Unorm, .rg8Unorm_sRGB:
            return 2
        case .rgba8Sint, .rgba8Uint, .rgba8Snorm, .rgba8Unorm, .rgba8Unorm_sRGB,
             .bgra8Unorm, .bgra8Unorm_sRGB:
            return 4
            
        // 16 bits per channel
        case .r16Sint, .r16Uint, .r16Float, .r16Snorm, .r16Unorm:
            return 2
        case .rg16Sint, .rg16Uint, .rg16Float, .rg16Snorm, .rg16Unorm:
            return 4
        case .rgba16Sint, .rgba16Uint, .rgba16Float, .rgba16Snorm, .rgba16Unorm:
            return 8
            
        // 32 bits per channel
        case .r32Sint, .r32Uint, .r32Float:
            return 4
            
        case .rg32Sint, .rg32Uint, .rg32Float:
            return 8
            
        case .rgba32Sint, .rgba32Uint, .rgba32Float:
            return 16
            
        // Packed 16-bit formats
        case .b5g6r5Unorm, .a1bgr5Unorm, .abgr4Unorm, .bgr5a1Unorm:
            return 2
            
        // Packed 32-bit formats
        case .rgb10a2Uint, .rgb10a2Unorm, .rg11b10Float, .rgb9e5Float,
             .bgr10a2Unorm, .bgr10_xr, .bgr10_xr_sRGB:
            return 4
            
        // Packed 64-bit formats
        case .bgra10_xr, .bgra10_xr_sRGB:
            return 8
            
        // Depth, stencil, and depth-stencil
        case .depth16Unorm:
            return 2
        case .depth32Float:
            return 4
        case .stencil8:
            return 1
        case .depth24Unorm_stencil8, .x24_stencil8:
            return 4
        case .depth32Float_stencil8, .x32_stencil8:
            return 5
            
        // BCn texture compression (reference: http://www.reedbeta.com/blog/understanding-bcn-texture-compression-formats)
        case .bc1_rgba, .bc1_rgba_sRGB:
            return 0.5
            
        case .bc4_rSnorm, .bc4_rUnorm:
            return 0.5
            
        case .bc3_rgba, .bc3_rgba_sRGB:
            return 1
            
        case .bc5_rgSnorm, .bc5_rgUnorm:
            return 1
            
        case .bc2_rgba, .bc2_rgba_sRGB:
            return 1
            
        case .bc6H_rgbFloat, .bc6H_rgbuFloat:
            return 1
            
        case .bc7_rgbaUnorm, .bc7_rgbaUnorm_sRGB:
            return 1
            
        // 4:2:2 subsampled
        case .gbgr422, .bgrg422:
            return 2
        }
    }
    
    @inlinable
    public var rowsPerBlock : Int {
        switch self  {
        case .bc1_rgba, .bc2_rgba, .bc3_rgba, .bc1_rgba_sRGB, .bc2_rgba_sRGB, .bc3_rgba_sRGB, .bc4_rSnorm, .bc4_rUnorm, .bc5_rgSnorm, .bc5_rgUnorm, .bc6H_rgbFloat, .bc6H_rgbuFloat, .bc7_rgbaUnorm, .bc7_rgbaUnorm_sRGB:
            return 4 // 4x4 blocks
        default:
            return 1
        }
    }
}

extension PixelFormat {
    public init?(string: String) {
        for i in 0...PixelFormat.x24_stencil8.rawValue {
            if let format = PixelFormat(rawValue: i) {
                if String(describing: format) == string {
                    self = format
                    return
                }
            }
        }
        return nil
    }

    public var isDepth : Bool {
        switch self {
        case .depth16Unorm, .depth32Float,
             .depth24Unorm_stencil8, .depth32Float_stencil8:
            return true
        default:
            return false
        }
    }
    
    public var isStencil : Bool {
        switch self {
        case .stencil8,
             .depth24Unorm_stencil8, .depth32Float_stencil8,
             .x24_stencil8, .x32_stencil8:
            return true
        default:
            return false
        }
    }
    
    public var isDepthStencil : Bool {
        switch self {
        case .depth24Unorm_stencil8, .depth32Float_stencil8:
            return true
        default:
            return false
        }
    }
    
    public var isUnnormalisedUInt: Bool {
        switch self {
        case .r8Uint, .rg8Uint, .rgba8Uint,
             .r16Uint, .rg16Uint, .rgba16Uint,
             .r32Uint, .rg32Uint, .rgba32Uint,
             .rgb10a2Uint:
            return true
        default:
            return false
        }
    }
    
    public var isUnnormalisedSInt: Bool {
        switch self {
        case .r8Sint, .rg8Sint, .rgba8Sint,
             .r16Sint, .rg16Sint, .rgba16Sint,
             .r32Sint, .rg32Sint, .rgba32Sint:
            return true
        default:
            return false
        }
    }
}

@available(OSX 10.11, *)
public enum CPUCacheMode : UInt, Hashable, Codable {
    
    
    case defaultCache
    
    case writeCombined
}

public enum StorageMode : UInt, Hashable, Codable {
    case shared
    case managed
    case `private`
}

public struct BlitOption : OptionSet, Hashable, Codable {
    public static let  depthFromDepthStencil = BlitOption(rawValue: 1 << 0)
    public static let  stencilFromDepthStencil = BlitOption(rawValue: 1 << 1)
    
    public let rawValue : UInt
    
    @inlinable
    public init(rawValue: UInt) {
        self.rawValue = rawValue
    }
}

public enum PrimitiveType : UInt, Hashable, Codable {
    
    case point
    
    case line
    
    case lineStrip
    
    case triangle
    
    case triangleStrip
}

public enum DataType : UInt, Hashable, Codable {
    
    case none
    
    case `struct`
    case array
    
    case float
    case float2
    case float3
    case float4
    
    case float2x2
    case float2x3
    case float2x4
    
    case float3x2
    case float3x3
    case float3x4
    
    case float4x2
    case float4x3
    case float4x4
    
    case half
    case half2
    case half3
    case half4
    
    case half2x2
    case half2x3
    case half2x4
    
    case half3x2
    case half3x3
    case half3x4
    
    case half4x2
    case half4x3
    case half4x4
    
    case int
    case int2
    case int3
    case int4
    
    case uint
    case uint2
    case uint3
    case uint4
    
    case short
    case short2
    case short3
    case short4
    
    case ushort
    case ushort2
    case ushort3
    case ushort4
    
    case char
    case char2
    case char3
    case char4
    
    case uchar
    case uchar2
    case uchar3
    case uchar4
    
    case bool
    case bool2
    case bool3
    case bool4
}

public enum VertexFormat : UInt, Hashable, Codable {
    
    
    case invalid
    
    
    case uchar2
    
    case uchar3
    
    case uchar4
    
    
    case char2
    
    case char3
    
    case char4
    
    
    case uchar2Normalized
    
    case uchar3Normalized
    
    case uchar4Normalized
    
    
    case char2Normalized
    
    case char3Normalized
    
    case char4Normalized
    
    
    case ushort2
    
    case ushort3
    
    case ushort4
    
    
    case short2
    
    case short3
    
    case short4
    
    
    case ushort2Normalized
    
    case ushort3Normalized
    
    case ushort4Normalized
    
    
    case short2Normalized
    
    case short3Normalized
    
    case short4Normalized
    
    
    case half2
    
    case half3
    
    case half4
    
    
    case float
    
    case float2
    
    case float3
    
    case float4
    
    
    case int
    
    case int2
    
    case int3
    
    case int4
    
    
    case uint
    
    case uint2
    
    case uint3
    
    case uint4
    
    
    case int1010102Normalized
    
    case uint1010102Normalized
}

public enum IndexType : UInt, Hashable, Codable {
    case uint16
    case uint32
}

public enum CullMode : UInt, Hashable, Codable {
    
    
    case none
    
    case front
    
    case back
}

public enum Winding : UInt, Hashable, Codable {
    
    
    case clockwise
    
    case counterClockwise
}

public enum DepthClipMode : UInt, Hashable, Codable {
    
    
    case clip
    
    case clamp
}

public enum TriangleFillMode : UInt, Hashable, Codable {
    
    
    case fill
    
    case lines
}

public enum CompareFunction : UInt, Hashable, Codable {
    
    
    case never
    
    case less
    
    case equal
    
    case lessEqual
    
    case greater
    
    case notEqual
    
    case greaterEqual
    
    case always
}

public enum StencilOperation : UInt, Hashable, Codable {
    
    
    case keep
    
    case zero
    
    case replace
    
    case incrementClamp
    
    case decrementClamp
    
    case invert
    
    case incrementWrap
    
    case decrementWrap
}

public struct Viewport : Hashable, Codable {
    
    public var originX: Double
    
    public var originY: Double
    
    public var width: Double
    
    public var height: Double
    
    public var zNear: Double
    
    public var zFar: Double
    
    public init(originX: Double, originY: Double, width: Double, height: Double, zNear: Double, zFar: Double) {
        self.originX = originX
        self.originY = originY
        self.width = width
        self.height = height
        self.zNear = zNear
        self.zFar = zFar
    }
}


public struct ClearColor : Hashable, Codable {
    
    public var red: Double
    
    public var green: Double
    
    public var blue: Double
    
    public var alpha: Double
    
    public init() {
        self.red = 0.0
        self.green = 0.0
        self.blue = 0.0
        self.alpha = 0.0
    }
    
    public init(red: Double, green: Double, blue: Double, alpha: Double) {
        self.red = red
        self.green = green
        self.blue = blue
        self.alpha = alpha
    }
}

public struct Origin : Hashable, Codable {
    
    public var x: Int
    
    public var y: Int
    
    public var z: Int
    
    public init() {
        self.x = 0
        self.y = 0
        self.z = 0
    }
    
    public init(x: Int, y: Int, z: Int) {
        self.x = x
        self.y = y
        self.z = z
    }
}

public struct Size : Hashable, Codable {
    
    public var width: Int
    
    public var height: Int
    
    public var depth: Int
    
    public init() {
        self.width = 0
        self.height = 0
        self.depth = 0
    }
    
    public init(width: Int, height: Int, depth: Int = 1) {
        self.width = width
        self.height = height
        self.depth = depth
    }
    
    
    public init(length: Int) {
        self.width = length
        self.height = 1
        self.depth = 1
    }
}

public struct Region : Hashable, Codable {
    
    public var origin: Origin
    
    public var size: Size
    
    public init() {
        self.origin = Origin()
        self.size = Size()
    }
    
    public init(origin: Origin, size: Size) {
        self.origin = origin
        self.size = size
    }
    
    public init(x: Int, y: Int, width: Int, height: Int) {
        self.origin = Origin(x: x, y: y, z: 0)
        self.size = Size(width: width, height: height, depth: 1)
    }
}

public struct ScissorRect : Hashable, Codable {
    
    public var x : Int = 0
    public var y : Int = 0
    public var width : Int = 0
    public var height : Int = 0
    
    public init(x: Int, y: Int, width: Int, height: Int) {
        self.x = x
        self.y = y
        self.width = width
        self.height = height
    }
    
    public static func ==(lhs: ScissorRect, rhs: ScissorRect) -> Bool {
        return lhs.x == rhs.x && lhs.y == rhs.y && lhs.width == rhs.width && lhs.height == rhs.height
    }
}
