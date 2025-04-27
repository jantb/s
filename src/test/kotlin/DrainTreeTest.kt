
import kotlin.test.Test
import kotlin.test.assertEquals

class DrainTreeTest {
    private fun createLogLineInn(body: String, timestamp: Long, severityNumber: LogLevel): LogLineInn {
        return LogLineInn(
            serviceName = "",
            body = body,
            timeUnixMillis = timestamp,
            logLevel = severityNumber,
            attributes = listOf("key" to "value"),
            severityText = Level.TRACE
        )
    }

    @Test
    fun testFinalizeSimple() {
        val drainTree = DrainTree()
        val logLine1 = createLogLineInn("Error in module A", 1000, LogLevel.INFO)
        val logLine2 = createLogLineInn("Error in module B", 1001, LogLevel.INFO)
        val logLine3 = createLogLineInn("Warning in module A", 1002, LogLevel.WARN)

        drainTree.add(logLine1)
        drainTree.add(logLine2)
        drainTree.add(logLine3)
        drainTree.final()

        val clusters = drainTree.logClusters()
        assertEquals(2, clusters.size)
    }

    @Test
    fun testFinalizeWithMultipleTokens() {
        val drainTree = DrainTree()
        val logLine1 = createLogLineInn("User 123 logged in", 2000, LogLevel.INFO)
        val logLine2 = createLogLineInn("User 456 logged out", 2001, LogLevel.INFO)
        val logLine3 = createLogLineInn("User 789 logged in", 2002, LogLevel.INFO)

        val one = drainTree.add(logLine1)
        val two = drainTree.add(logLine2)
        val three = drainTree.add(logLine3)

        assertEquals(logLine1.body, drainTree.get(one).body)
        assertEquals(logLine1.timeUnixMillis, drainTree.get(one).timestamp)
        assertEquals(logLine2.body, drainTree.get(two).body)
        assertEquals(logLine2.timeUnixMillis, drainTree.get(two).timestamp)
        assertEquals(logLine3.body, drainTree.get(three).body)
        assertEquals(logLine3.timeUnixMillis, drainTree.get(three).timestamp)

        drainTree.final()

        val clusters = drainTree.logClusters()
        assertEquals(1, clusters.size)
        assertEquals(3, clusters[0].first)

        assertEquals(logLine1.body, drainTree.get(one).body)
        assertEquals(logLine1.timeUnixMillis, drainTree.get(one).timestamp)
        assertEquals(logLine2.body, drainTree.get(two).body)
        assertEquals(logLine2.timeUnixMillis, drainTree.get(two).timestamp)
        assertEquals(logLine3.body, drainTree.get(three).body)
        assertEquals(logLine3.timeUnixMillis, drainTree.get(three).timestamp)
    }

    @Test
    fun testGetAfterFinalize() {
        val drainTree = DrainTree()
        val logLine1 = createLogLineInn("Error in module X", 3000, LogLevel.ERROR)
        val logLine2 = createLogLineInn("Error in module Y", 3001, LogLevel.ERROR)

        drainTree.add(logLine1)
        drainTree.add(logLine2)

        drainTree.final()

        val logPart1 = drainTree.get(0)
        assertEquals("Error in module X", logPart1.body)
        assertEquals(LogLevel.ERROR, logPart1.level)

        val logPart2 = drainTree.get(1)
        assertEquals("Error in module Y", logPart2.body)
        assertEquals(LogLevel.ERROR, logPart2.level)
    }

    @Test
    fun testGetAfterFinalizeUuid() {
        val drainTree = DrainTree()
        val logLine1 = createLogLineInn(
            "Error in module 4ff93c88-08ed-4b9c-9e9a-0c820b793d48",
            3000,
            LogLevel.ERROR
        )
        val logLine2 = createLogLineInn(
            "Error in module 6ff93c88-08ed-4b9c-9e9a-0c820b793d48",
            3001,
            LogLevel.ERROR
        )
        val logLine3 = createLogLineInn(
            "Error in module 2ff93c88-08ed-4b9c-9e9a-0c820b793d48",
            3001,
            LogLevel.ERROR
        )

        drainTree.add(logLine1)
        drainTree.add(logLine2)
        drainTree.add(logLine3)

        drainTree.final()

        val logPart1 = drainTree.get(0)
        assertEquals("Error in module 4ff93c88-08ed-4b9c-9e9a-0c820b793d48", logPart1.body)
        assertEquals(LogLevel.ERROR, logPart1.level)

        val logPart2 = drainTree.get(1)
        assertEquals("Error in module 6ff93c88-08ed-4b9c-9e9a-0c820b793d48", logPart2.body)
        assertEquals(LogLevel.ERROR, logPart2.level)

        val logPart3 = drainTree.get(2)
        assertEquals("Error in module 2ff93c88-08ed-4b9c-9e9a-0c820b793d48", logPart3.body)
        assertEquals(LogLevel.ERROR, logPart3.level)
    }
}