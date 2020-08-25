import CAtomics

extension UInt {
    @inlinable
    public subscript(bit at: Int) -> Bool {
        get {
            return (self &>> at) & 0b1 != 0
        } set {
            if newValue {
                self |= (1 &<< at)
            } else {
                self &= ~(1 &<< at)
            }
        }
    }
}

extension UInt64 {
    @inlinable
    public subscript(bit at: Int) -> Bool {
        get {
            return (self &>> at) & 0b1 != 0
        } set {
            if newValue {
                self |= (1 &<< at)
            } else {
                self &= ~(1 &<< at)
            }
        }
    }
}

public struct BitSet {
    @usableFromInline let storage : UnsafeMutablePointer<UInt>
    @usableFromInline let storageCount : Int
    
    @inlinable static var bitsPerElement : Int {
        return UInt.bitWidth
    }
    
    @inlinable
    public init(storageCount: Int, allocator: AllocatorType = .system) {
        self.storage = Allocator.allocate(capacity: storageCount, allocator: allocator)
        self.storage.initialize(repeating: 0, count: storageCount)
        self.storageCount = storageCount
    }
    
    @inlinable
    public init(storage: UnsafeMutableBufferPointer<UInt>) {
        self.storage = storage.baseAddress!
        self.storageCount = storage.count
    }
    
    public func dispose(allocator: AllocatorType = .system) {
        Allocator.deallocate(self.storage, allocator: allocator)
    }
    
    @inlinable
    public subscript(uintIndex uintIndex: Int, offset offset: Int) -> UInt {
        get {
            var result = 0 as UInt
            result |= self.storage[uintIndex] >> offset // contributes the lower (64 - offset) bits
            if offset > 0, uintIndex + 1 < self.storageCount {
                let remainingBits = BitSet.bitsPerElement - offset
                let upperBits = self.storage[uintIndex + 1] // contributes the upper (offset) bits
                result |= upperBits << remainingBits
            }
            return result
        }
        nonmutating set {
            self.storage[uintIndex] &= ~(~0 << offset) // zero out offset..<64 at uintIndex
            self.storage[uintIndex] |= newValue << offset // set offset..<64 to newValue
            
            if offset > 0, uintIndex + 1 < self.storageCount {
                let remainingBits = BitSet.bitsPerElement - offset
                self.storage[uintIndex + 1] &= ~((1 << offset) - 1) // zero out 0..<offset at uintIndex + 1
                self.storage[uintIndex + 1] |= newValue >> remainingBits
            }
        }
    }
    
    @inlinable
    public subscript(bitIndex: Int) -> Bool {
        get {
            let (uintIndex, offset) = bitIndex.quotientAndRemainder(dividingBy: BitSet.bitsPerElement)
            return self.storage[uintIndex][bit: offset]
        }
        nonmutating set {
            let (uintIndex, offset) = bitIndex.quotientAndRemainder(dividingBy: BitSet.bitsPerElement)
            self.storage[uintIndex][bit: offset] = newValue
        }
    }

    @inlinable
    public func setBits(in range: Range<Int>) {
        assert(range.upperBound <= self.storageCount * BitSet.bitsPerElement)
        
        var (uintIndex, offset) = range.lowerBound.quotientAndRemainder(dividingBy: BitSet.bitsPerElement)
        
        var remaining = range.count
        
        let bitsInFirstWordCount = min(range.count, BitSet.bitsPerElement - offset)
        let firstWordBits : UInt = (bitsInFirstWordCount == BitSet.bitsPerElement) ? ~0 : ((1 &<< bitsInFirstWordCount) &- 1)
        self.storage[uintIndex] |= firstWordBits << offset
        
        uintIndex += 1
        remaining -= bitsInFirstWordCount
        
        while remaining > 0 {
            if remaining < BitSet.bitsPerElement {
                self.storage[uintIndex] |= (1 &<< remaining) &- 1
                remaining = 0
            } else {
                self.storage[uintIndex] = ~0
                remaining -= BitSet.bitsPerElement
            }
            uintIndex += 1
            
        }
        
    }
    
    @inlinable
    public func clearBits(in set: BitSet) {
        assert(set.storageCount == self.storageCount)
        for i in 0..<self.storageCount {
            self.storage[i] &= ~set.storage[i]
        }
    }
    
    @inlinable
    public func clear() {
        self.storage.assign(repeating: 0, count: self.storageCount)
    }
    
    @inlinable
    public var isEmpty : Bool {
        for i in 0..<self.storageCount {
            if storage[i] != 0 {
                return false
            }
        }
        return true
    }
}

public struct AtomicBitSet {
    @usableFromInline let storage : UnsafeMutablePointer<AtomicUInt>
    @usableFromInline let storageCount : Int
    
    @inlinable static var bitsPerElement : Int {
        return UInt.bitWidth
    }
    
    @inlinable
    public init(storageCount: Int) {
        self.storage = .allocate(capacity: storageCount)
        self.storage.initialize(repeating: AtomicUInt(0), count: storageCount)
        self.storageCount = storageCount
    }
    
    @inlinable
    public subscript(uintIndex uintIndex: Int, offset offset: Int) -> UInt {
        get {
            var result = 0 as UInt
            result |= CAtomicsLoad(self.storage.advanced(by: uintIndex), .relaxed) >> offset // contributes the lower (64 - offset) bits
            if offset > 0, uintIndex + 1 < self.storageCount {
                let remainingBits = BitSet.bitsPerElement - offset
                let upperBits = CAtomicsLoad(self.storage.advanced(by: uintIndex + 1), .relaxed)// contributes the upper (offset) bits
                result |= upperBits << remainingBits
            }
            return result
        }
        nonmutating set {
            CAtomicsBitwiseAnd(self.storage.advanced(by: uintIndex), ~(~0 << offset), .relaxed) // zero out offset..<64 at uintIndex
            CAtomicsBitwiseOr(self.storage.advanced(by: uintIndex), newValue << offset, .relaxed) // set offset..<64 to newValue
            
            if offset > 0, uintIndex + 1 < self.storageCount {
                let remainingBits = BitSet.bitsPerElement - offset

                CAtomicsBitwiseAnd(self.storage.advanced(by: uintIndex + 1), ~((1 << offset) - 1), .relaxed) // zero out 0..<offset at uintIndex + 1
                CAtomicsBitwiseOr(self.storage.advanced(by: uintIndex + 1), newValue >> remainingBits, .relaxed) // set offset..<64 to newValue
            }
        }
    }
    
//    @inlinable
//    public subscript(bitIndex: Int) -> Bool {
//        get {
//            let (uintIndex, offset) = bitIndex.quotientAndRemainder(dividingBy: BitSet.bitsPerElement)
//            return self.storage[uintIndex][bit: offset]
//        }
//        nonmutating set {
//            let (uintIndex, offset) = bitIndex.quotientAndRemainder(dividingBy: BitSet.bitsPerElement)
//            self.storage[uintIndex][bit: offset] = newValue
//        }
//    }

    @inlinable
    public func testBitsAreClear(in range: Range<Int>) -> Bool {
        assert(range.upperBound <= self.storageCount * BitSet.bitsPerElement)
        
        var (uintIndex, offset) = range.lowerBound.quotientAndRemainder(dividingBy: BitSet.bitsPerElement)
        
        var remaining = range.count
        
        let bitsInFirstWordCount = min(range.count, BitSet.bitsPerElement - offset)
        let firstWordBits : UInt = (bitsInFirstWordCount == BitSet.bitsPerElement) ? ~0 : ((1 &<< bitsInFirstWordCount) &- 1)
        if CAtomicsLoad(self.storage.advanced(by: uintIndex), .relaxed) & (firstWordBits << offset) != 0 {
            return false
        }
        
        uintIndex += 1
        remaining -= bitsInFirstWordCount
        
        while remaining > 0 {
            if remaining < BitSet.bitsPerElement {
                if CAtomicsLoad(self.storage.advanced(by: uintIndex), .relaxed) & ((1 &<< remaining) &- 1) != 0 {
                    return false
                }
                remaining = 0
            } else {
                if CAtomicsLoad(self.storage.advanced(by: uintIndex), .relaxed) != 0 {
                    return false
                }
                remaining -= BitSet.bitsPerElement
            }
            uintIndex += 1
        }
        return true
    }
    
    @inlinable
    public func setBits(in range: Range<Int>) {
        assert(range.upperBound <= self.storageCount * BitSet.bitsPerElement)
        
        var (uintIndex, offset) = range.lowerBound.quotientAndRemainder(dividingBy: BitSet.bitsPerElement)
        
        var remaining = range.count
        
        let bitsInFirstWordCount = min(range.count, BitSet.bitsPerElement - offset)
        let firstWordBits : UInt = (bitsInFirstWordCount == BitSet.bitsPerElement) ? ~0 : ((1 &<< bitsInFirstWordCount) &- 1)
        CAtomicsBitwiseOr(self.storage.advanced(by: uintIndex), firstWordBits << offset, .relaxed)
        
        uintIndex += 1
        remaining -= bitsInFirstWordCount
        
        while remaining > 0 {
            if remaining < BitSet.bitsPerElement {
                CAtomicsBitwiseOr(self.storage.advanced(by: uintIndex), (1 &<< remaining) &- 1, .relaxed)
                remaining = 0
            } else {
                CAtomicsStore(self.storage.advanced(by: uintIndex), ~0, .relaxed)
                remaining -= BitSet.bitsPerElement
            }
            uintIndex += 1
            
        }
    }
    
    @inlinable
    public func clearBits(in set: BitSet) {
        assert(set.storageCount == self.storageCount)
        for i in 0..<self.storageCount {
            CAtomicsBitwiseAnd(self.storage.advanced(by: i), ~set.storage[i], .relaxed)
        }
    }
    
    @inlinable
    public func clear() {
        for i in 0..<self.storageCount {
            CAtomicsStore(self.storage.advanced(by: i), 0, .relaxed)
        }
    }
    
    @inlinable
    public var isEmpty : Bool {
        for i in 0..<self.storageCount {
            if CAtomicsLoad(storage.advanced(by: i), .relaxed) != 0 {
                return false
            }
        }
        return true
    }
}

