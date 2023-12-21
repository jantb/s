package util

import kotlin.test.Test
import kotlin.test.assertEquals

class NextPowerOf2Test {
    @Test
    fun testNextPowerOf2() {
        assertEquals(1, 1.nextPowerOf2(), "next power of 2 for 1 should be 1")
        assertEquals(2, 2.nextPowerOf2(), "next power of 2 for 2 should be 2")
        assertEquals(4, 3.nextPowerOf2(), "next power of 2 for 3 should be 4")
        assertEquals(16, 10.nextPowerOf2(), "next power of 2 for 10 should be 16")
        assertEquals(1024, 1000.nextPowerOf2(), "next power of 2 for 1000 should be 1024")
    }
}