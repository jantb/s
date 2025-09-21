package widgets

import ComponentOwn
import LogLevel
import SlidePanel
import State
import util.Styles
import util.UiColors
import java.awt.Color
import java.awt.Font
import java.awt.Graphics2D
import java.awt.Rectangle
import java.awt.RenderingHints
import java.awt.event.KeyEvent
import java.awt.event.KeyListener
import java.awt.event.MouseEvent
import java.awt.event.MouseListener
import java.awt.event.MouseMotionListener
import java.awt.event.MouseWheelEvent
import java.awt.event.MouseWheelListener
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import javax.swing.SwingUtilities
import javax.swing.Timer
import kotlinx.coroutines.*
import kotlin.math.min
import kotlin.time.ExperimentalTime

/**
 * DashboardView that queries DashboardService with the correct normalized intervals and
 * avoids double counting throughput (uses max(podSum, queueSum) per bucket).
 *
 * Throughput chart:
 * - Requests data bucketed by fixed seconds depending on the selected time range.
 * - Computes per-bucket rate = messagesInBucket / bucketSizeSeconds, where messagesInBucket is
 *   max(sumPod, sumQueue) to avoid double counting.
 *
 * Log-level chart and pod throughput chart:
 * - Always use DashboardService, which normalizes minutes to supported intervals.
 */
class DashboardView(
    private val panel: SlidePanel,
    x: Int,
    y: Int,
    width: Int,
    height: Int
) : ComponentOwn(),
    KeyListener,
    MouseListener,
    MouseWheelListener,
    MouseMotionListener {

    private val dashboardService = DashboardService.getInstance()

    private var visibleLines = 0
    private var rowHeight = 12
    private lateinit var image: BufferedImage
    private lateinit var g2d: Graphics2D
    private var mouseposX = 0
    private var mouseposY = 0
    private var maxCharBounds: Rectangle2D? = null

    // Time range selection
    private var selectedTimeRange = TimeRange.FIVE_MINUTES
    private val timeRangeButtons = mutableListOf<Rectangle>()

    // Chart properties
    private var chartHeight = 200
    private var tooltipInfo: TooltipInfo? = null
    private var hoveredPodBar: String? = null
    private var podTooltipInfo: PodTooltipInfo? = null
    private var throughputTooltipInfo: ThroughputTooltipInfo? = null

    // Pre-calculated fonts
    private val headerBoldFont = Font(Styles.normalFont, Font.BOLD, 14)
    private val headerPlainFont = Font(Styles.normalFont, Font.PLAIN, 12)
    private val tooltipFont = Font(Styles.normalFont, Font.BOLD, 12)
    private val tooltipPlainFont = Font(Styles.normalFont, Font.PLAIN, 11)
    private val metricFont = Font(Styles.normalFont, Font.BOLD, 16)

    // Auto-refresh timer for updating dashboard data every 30 seconds
    private val refreshTimer = Timer(30000) {
        refreshDashboardData()
    }

    private data class TooltipInfo(
        val x: Int,
        val y: Int,
        val level: LogLevel,
        val count: Int,
        val percentage: Float
    )

    private data class PodTooltipInfo(
        val x: Int,
        val y: Int,
        val podName: String,
        val throughput: Double,
        val isActive: Boolean
    )

    private data class ThroughputTooltipInfo(
        val x: Int,
        val y: Int,
        val throughput: Double,
        val timestamp: String
    )

    private enum class TimeRange(val minutes: Int, val displayName: String) {
        FIVE_MINUTES(5, "5m"),
        FIFTEEN_MINUTES(15, "15m"),
        ONE_HOUR(60, "1h"),
        SIX_HOURS(360, "6h"),
        TWENTY_FOUR_HOURS(1440, "24h")
    }

    init {
        this.x = x
        this.y = y
        this.width = width
        this.height = height

        // Start the auto-refresh timer (updates every 30 seconds)
        refreshTimer.start()

        // Perform initial refresh when component is created
        refreshDashboardData()

        // Wire UI repaint to data updates
        dashboardService.setOnDataUpdateCallback {
            SwingUtilities.invokeLater { panel.repaint() }
        }

        // Initialize selected time range in the service (normalized internally)
        dashboardService.setSelectedTimeRange(selectedTimeRange.minutes)

        // Periodic repaint safeguard
        Thread {
            while (true) {
                try {
                    Thread.sleep(1000)
                    SwingUtilities.invokeLater { panel.repaint() }
                } catch (_: InterruptedException) {
                    break
                }
            }
        }.apply {
            isDaemon = true
            start()
        }
    }

    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        if (this.width != width || this.height != height || this.x != x || this.y != y) {
            try {
                if (::g2d.isInitialized) g2d.dispose()
            } catch (_: Exception) {
                // ignore
            }
            this.image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
            this.g2d = image.createGraphics().apply {
                setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
                setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON)
                setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY)
            }
            this.height = height
            this.width = width
            this.x = x
            this.y = y
        }

        if (!::g2d.isInitialized) {
            this.image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
            this.g2d = image.createGraphics().apply {
                setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
                setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON)
                setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY)
            }
        }

        // Clear
        g2d.color = UiColors.background
        g2d.fillRect(0, 0, width, height)
        g2d.font = Font(Styles.normalFont, Font.PLAIN, rowHeight)
        maxCharBounds = g2d.fontMetrics.getMaxCharBounds(g2d)

        g2d.color = UiColors.magenta
        paint()
        return image
    }

    override fun repaint(componentOwn: ComponentOwn) {
        // No-op
    }

    private fun paint() {
        g2d.color = UiColors.defaultText

        // Calculate chart height and visible lines
        chartHeight = 200
        val charHeight = maxCharBounds?.height?.toInt() ?: 12
        visibleLines = ((height - chartHeight - 30) / charHeight) - 2

        drawTimeRangeButtons()
        drawMetricsCards()
        drawThroughputChart()
        drawLogLevelChart()
        drawPodThroughputChart()
        drawKafkaLagInfo()
        drawTooltip()
    }

    private fun drawTimeRangeButtons() {
        val buttonY = 5
        val buttonHeight = 20
        val buttonSpacing = 5
        var buttonX = 10

        timeRangeButtons.clear()

        TimeRange.entries.forEach { timeRange ->
            val buttonWidth = g2d.fontMetrics.stringWidth(timeRange.displayName) + 10
            val buttonRect = Rectangle(buttonX, buttonY, buttonWidth, buttonHeight)
            timeRangeButtons.add(buttonRect)

            val isSelected = timeRange == selectedTimeRange
            g2d.color =
                if (isSelected) UiColors.selection
                else Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 150)
            g2d.fillRoundRect(buttonX, buttonY, buttonWidth, buttonHeight, 4, 4)

            g2d.color = if (isSelected) UiColors.selection.darker() else UiColors.defaultText.darker()
            g2d.drawRoundRect(buttonX, buttonY, buttonWidth, buttonHeight, 4, 4)

            g2d.font = Font(Styles.normalFont, Font.BOLD, 10)
            g2d.color = if (isSelected) UiColors.defaultText else UiColors.defaultText.darker()
            g2d.drawString(timeRange.displayName, buttonX + 5, buttonY + 14)

            buttonX += buttonWidth + buttonSpacing
        }
    }

    private fun drawMetricsCards() {
        val currentMetrics = dashboardService.getDashboardMetrics()

        val cardWidth = 200
        val cardHeight = 80
        val cardSpacing = 20
        val startY = 35

        val metrics = listOf(
            Triple("Total Messages", currentMetrics.totalMessages.format(), UiColors.teal),
            Triple("Avg Throughput", String.format("%.1f/s", currentMetrics.averageThroughput), UiColors.green),
            Triple("Error Rate", String.format("%.1f%%", currentMetrics.errorRate), UiColors.red),
            Triple("Active Pods", currentMetrics.activePods.toString(), UiColors.magenta)
        )

        metrics.forEachIndexed { index, (label, value, color) ->
            val cardX = 10 + index * (cardWidth + cardSpacing)

            g2d.color = Color(color.red, color.green, color.blue, 30)
            g2d.fillRoundRect(cardX, startY, cardWidth, cardHeight, 10, 10)

            g2d.color = color.darker()
            g2d.drawRoundRect(cardX, startY, cardWidth, cardHeight, 10, 10)

            g2d.font = headerPlainFont
            g2d.color = UiColors.defaultText
            g2d.drawString(label, cardX + 10, startY + 20)

            g2d.font = metricFont
            g2d.color = color
            g2d.drawString(value, cardX + 10, startY + 50)

            g2d.font = Font(Styles.normalFont, Font.BOLD, 10)
            g2d.color = UiColors.green
            g2d.drawString("↗", cardX + cardWidth - 20, startY + 20)
        }
    }

    @OptIn(ExperimentalTime::class)
    private fun drawThroughputChart() {
        val chartY = 130
        val chartHeight = 80

        g2d.color = Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200)
        g2d.fillRect(10, chartY, width - 20, chartHeight)

        g2d.font = headerBoldFont
        g2d.color = UiColors.defaultText.brighter()
        g2d.drawString("Throughput (Last ${selectedTimeRange.displayName})", 20, chartY + 20)

        // Pick a UI bucket size per selected range to avoid huge point counts
        val bucketSizeSeconds = when (selectedTimeRange) {
            TimeRange.FIVE_MINUTES -> 5   // 5s buckets
            TimeRange.FIFTEEN_MINUTES -> 30
            TimeRange.ONE_HOUR -> 60
            TimeRange.SIX_HOURS -> 300
            TimeRange.TWENTY_FOUR_HOURS -> 600
        }

        // Request time-bucketed data for the selected range (DashboardService normalizes internally)
        val throughputData = dashboardService.getTimeBucketedThroughputData(
            minutes = selectedTimeRange.minutes,
            bucketSizeSeconds = bucketSizeSeconds
        )

        if (throughputData.isNotEmpty()) {
            // messages per bucket = max(podSum, queueSum) to avoid double counting
            val perBucketRates = throughputData.map { dp ->
                val pod = dp.podThroughput.values.sum()
                val queue = dp.queueThroughput.values.sum()
                val messages = maxOf(pod, queue).toDouble()
                messages / bucketSizeSeconds.toDouble()
            }

            val maxThroughput = (perBucketRates.maxOrNull() ?: 0.0).coerceAtLeast(1.0)
            val chartWidth = width - 40
            val barWidth = (chartWidth / perBucketRates.size.toFloat()).coerceAtLeast(1f)

            perBucketRates.forEachIndexed { index, rate ->
                val barHeight = ((rate / maxThroughput) * (chartHeight - 40)).toInt().coerceAtLeast(0)
                val barX = 20 + (index * barWidth).toInt()
                val barY = chartY + chartHeight - 20 - barHeight
                val barRect = Rectangle(barX, barY, barWidth.toInt(), barHeight)

                if (barRect.contains(mouseposX, mouseposY)) {
                    g2d.color = UiColors.teal.brighter()
                    throughputTooltipInfo = ThroughputTooltipInfo(
                        x = mouseposX,
                        y = mouseposY,
                        throughput = rate,
                        timestamp = throughputData[index].messageTimestamp.toString()
                    )
                } else {
                    g2d.color = UiColors.teal
                }
                g2d.fillRect(barX, barY, barWidth.toInt(), barHeight)

                g2d.color = UiColors.teal.darker()
                g2d.drawRect(barX, barY, barWidth.toInt(), barHeight)
            }
        } else {
            g2d.font = headerPlainFont
            g2d.color = UiColors.defaultText.darker()
            g2d.drawString("No throughput data available", 20, chartY + 40)
        }

        g2d.color = UiColors.defaultText.darker()
        g2d.drawRect(10, chartY, width - 20, chartHeight)
    }

    private fun drawLogLevelChart() {
        val chartY = 230
        val chartHeight = 80

        g2d.color = Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200)
        g2d.fillRect(10, chartY, width - 20, chartHeight)

        g2d.font = headerBoldFont
        g2d.color = UiColors.defaultText.brighter()
        g2d.drawString("Log Level Distribution", 20, chartY + 20)

        val logLevelData = dashboardService.getLogLevelData(selectedTimeRange.minutes)

        if (logLevelData.isNotEmpty()) {
            val selectedLevels = State.levels.get()
            val levelCounts: Map<LogLevel, Long> =
                logLevelData
                    .flatMap { it.counts.entries }
                    .filter { it.key in selectedLevels }
                    .groupBy({ it.key }, { it.value })
                    .mapValues { (_, counts) -> counts.sum() }

            val total = levelCounts.values.sum()
            val maxCount = (levelCounts.values.maxOrNull() ?: 0L).coerceAtLeast(1L)
            val barWidth = if (selectedLevels.isNotEmpty()) (width - 40) / selectedLevels.size.toFloat() else 0f

            selectedLevels.forEachIndexed { index, level ->
                val count = levelCounts[level] ?: 0L
                val barHeight = if (total > 0) ((count.toDouble() / maxCount) * (chartHeight - 40)).toInt() else 0

                val barX = 20 + (index * barWidth).toInt()
                val barY = chartY + chartHeight - 20 - barHeight

                g2d.color = getLevelColor(level)
                g2d.fillRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)

                g2d.color = getLevelColor(level).darker()
                g2d.drawRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)

                g2d.font = Font(Styles.normalFont, Font.PLAIN, 10)
                g2d.color = UiColors.defaultText
                g2d.drawString(level.name, barX + 2, chartY + chartHeight - 5)
            }
        } else {
            g2d.font = headerPlainFont
            g2d.color = UiColors.defaultText.darker()
            g2d.drawString("No log level data available", 20, chartY + 40)
        }

        g2d.color = UiColors.defaultText.darker()
        g2d.drawRect(10, chartY, width - 20, chartHeight)
    }

    private fun drawPodThroughputChart() {
        val chartY = 330
        val chartHeight = 120

        g2d.color = Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200)
        g2d.fillRect(10, chartY, width - 20, chartHeight)

        g2d.font = headerBoldFont
        g2d.color = UiColors.defaultText.brighter()
        g2d.drawString("Pod Throughput (Last ${selectedTimeRange.displayName})", 20, chartY + 20)

        val perPodRates: Map<String, Double> =
            if (selectedTimeRange == TimeRange.FIVE_MINUTES)
                dashboardService.getCurrentPodThroughput() // ~last 60s average
            else
                dashboardService.getPodThroughputRates(selectedTimeRange.minutes)

        val activePods = dashboardService.getActivePods()
        val podThroughputData = perPodRates.filterKeys { it.isNotBlank() }

        if (podThroughputData.isNotEmpty()) {
            val maxThroughput = (podThroughputData.values.maxOrNull() ?: 0.0).coerceAtLeast(1.0)
            val barWidth = (width - 60) / podThroughputData.size.toFloat()
            val maxBarHeight = chartHeight - 60

            podThroughputData.entries.forEachIndexed { index, (podName, throughput) ->
                val barHeight = ((throughput / maxThroughput) * maxBarHeight).toInt().coerceAtLeast(0)
                val barX = 20 + (index * barWidth).toInt()
                val barY = chartY + chartHeight - 30 - barHeight

                val isActive = activePods.contains(podName)
                g2d.color = if (isActive) UiColors.green else UiColors.teal

                val barRect = Rectangle(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)
                if (hoveredPodBar == podName) {
                    g2d.color = g2d.color.brighter()
                    podTooltipInfo = PodTooltipInfo(
                        x = mouseposX,
                        y = mouseposY,
                        podName = podName,
                        throughput = throughput,
                        isActive = isActive
                    )
                }

                g2d.fillRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)
                g2d.color = g2d.color.darker()
                g2d.drawRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)

                // Pod name label truncated
                g2d.font = Font(Styles.normalFont, Font.PLAIN, 9)
                g2d.color = UiColors.defaultText
                val availableWidth = barWidth.toInt().coerceAtLeast(1) - 4
                val maxChars = (availableWidth / g2d.fontMetrics.charWidth('A')).coerceAtMost(podName.length).coerceAtLeast(0)
                val displayName = if (podName.length > maxChars && maxChars > 3) podName.take(maxChars - 3) + "..." else podName
                g2d.drawString(displayName, barX + 2, chartY + chartHeight - 10)

                val labelRect = Rectangle(barX, chartY + chartHeight - 20, barWidth.toInt().coerceAtLeast(1), 15)
                if (labelRect.contains(mouseposX, mouseposY)) {
                    podTooltipInfo = PodTooltipInfo(
                        x = mouseposX,
                        y = mouseposY,
                        podName = podName,
                        throughput = throughput,
                        isActive = isActive
                    )
                }

                if (barHeight > 15) {
                    g2d.font = Font(Styles.normalFont, Font.BOLD, 8)
                    g2d.color = UiColors.defaultText
                    g2d.drawString(String.format("%.1f", throughput), barX + 2, barY - 2)
                }
            }
        } else {
            g2d.font = headerPlainFont
            g2d.color = UiColors.defaultText.darker()
            g2d.drawString("No pod throughput data available", 20, chartY + 40)
        }

        g2d.color = UiColors.defaultText.darker()
        g2d.drawRect(10, chartY, width - 20, chartHeight)
    }

    private fun drawKafkaLagInfo() {
        val lagY = 470
        val lagHeight = height - lagY - 10

        g2d.color = Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200)
        g2d.fillRect(10, lagY, width - 20, lagHeight)

        g2d.font = headerBoldFont
        g2d.color = UiColors.defaultText.brighter()
        g2d.drawString("Kafka Lag Information", 20, lagY + 20)

        val lagSummary = dashboardService.getKafkaLagSummary()
        val partitionDetails = dashboardService.getPartitionLagDetails()

        if (partitionDetails.isNotEmpty()) {
            g2d.font = headerPlainFont
            g2d.color = UiColors.teal
            g2d.drawString("High Lag: ${lagSummary["high"] ?: 0}", 20, lagY + 40)
            g2d.color = UiColors.orange
            g2d.drawString("Medium Lag: ${lagSummary["medium"] ?: 0}", 20, lagY + 55)
            g2d.color = UiColors.green
            g2d.drawString("Low Lag: ${lagSummary["low"] ?: 0}", 20, lagY + 70)

            g2d.font = Font(Styles.normalFont, Font.BOLD, 12)
            g2d.color = UiColors.magenta
            g2d.drawString("Topic", 20, lagY + 95)
            g2d.drawString("Partition", 150, lagY + 95)
            g2d.drawString("Lag", 220, lagY + 95)
            g2d.drawString("Group", 280, lagY + 95)

            g2d.color = UiColors.defaultText.darker()
            g2d.drawLine(20, lagY + 100, width - 30, lagY + 100)

            g2d.font = Font(Styles.normalFont, Font.PLAIN, 11)
            val maxVisibleItems = min(20, partitionDetails.size)

            for (i in 0 until maxVisibleItems) {
                val partition = partitionDetails[i]
                val y = lagY + 115 + i * 15

                g2d.color = UiColors.defaultText
                g2d.drawString(partition["topic"] as String, 20, y)
                g2d.drawString((partition["partition"] as Int).toString(), 150, y)

                val lag = partition["lag"] as Long
                g2d.color = when {
                    lag >= 100 -> UiColors.red
                    lag >= 10 -> UiColors.orange
                    else -> UiColors.green
                }
                g2d.drawString(lag.toString(), 220, y)

                g2d.color = UiColors.defaultText
                g2d.drawString(partition["groupId"] as String, 280, y)
            }
        } else {
            g2d.font = headerPlainFont
            g2d.color = UiColors.defaultText.darker()
            g2d.drawString("No Kafka lag data available", 20, lagY + 40)
        }

        g2d.color = UiColors.defaultText.darker()
        g2d.drawRect(10, lagY, width - 20, lagHeight)
    }

    private fun drawTooltip() {
        tooltipInfo?.let { info ->
            g2d.font = tooltipFont
            val titleWidth = g2d.fontMetrics.stringWidth(info.level.name)
            g2d.font = tooltipPlainFont
            val countText = "Count: ${info.count}"
            val shareText = "Share: ${String.format("%.1f%%", info.percentage)}"
            val maxTextWidth = maxOf(titleWidth, g2d.fontMetrics.stringWidth(countText), g2d.fontMetrics.stringWidth(shareText))
            val tooltipWidth = maxOf(120, maxTextWidth + 20)
            val tooltipHeight = 50
            val tooltipX = min(info.x + 10, width - tooltipWidth)
            val tooltipY = min(info.y - tooltipHeight - 10, height - tooltipHeight)

            g2d.color = Color(0, 0, 0, 200)
            g2d.fillRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)
            g2d.color = UiColors.defaultText
            g2d.drawRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)

            g2d.font = tooltipFont
            g2d.color = getLevelColor(info.level)
            g2d.drawString(info.level.name, tooltipX + 5, tooltipY + 15)

            g2d.font = tooltipPlainFont
            g2d.color = UiColors.defaultText
            g2d.drawString(countText, tooltipX + 5, tooltipY + 30)
            g2d.drawString(shareText, tooltipX + 5, tooltipY + 42)
        }

        podTooltipInfo?.let { info ->
            g2d.font = tooltipFont
            val podNameWidth = g2d.fontMetrics.stringWidth(info.podName)
            g2d.font = tooltipPlainFont
            val throughputText = "Throughput: ${String.format("%.1f/s", info.throughput)}"
            val statusText = "Status: ${if (info.isActive) "Active" else "Inactive"}"
            val maxTextWidth = maxOf(podNameWidth, g2d.fontMetrics.stringWidth(throughputText), g2d.fontMetrics.stringWidth(statusText))
            val tooltipWidth = maxOf(140, maxTextWidth + 20)
            val tooltipHeight = 55
            val tooltipX = min(info.x + 10, width - tooltipWidth)
            val tooltipY = min(info.y - tooltipHeight - 10, height - tooltipHeight)

            g2d.color = Color(0, 0, 0, 200)
            g2d.fillRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)
            g2d.color = UiColors.defaultText
            g2d.drawRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)

            g2d.font = tooltipFont
            g2d.color = if (info.isActive) UiColors.green else UiColors.teal
            g2d.drawString(info.podName, tooltipX + 5, tooltipY + 15)

            g2d.font = tooltipPlainFont
            g2d.color = UiColors.defaultText
            g2d.drawString(throughputText, tooltipX + 5, tooltipY + 30)
            g2d.drawString(statusText, tooltipX + 5, tooltipY + 45)
        }

        throughputTooltipInfo?.let { info ->
            g2d.font = tooltipFont
            val titleWidth = g2d.fontMetrics.stringWidth("Throughput")
            g2d.font = tooltipPlainFont
            val throughputText = String.format("%.1f/s", info.throughput)
            val maxTextWidth = maxOf(titleWidth, g2d.fontMetrics.stringWidth(throughputText))
            val tooltipWidth = maxOf(100, maxTextWidth + 20)
            val tooltipHeight = 40
            val tooltipX = min(info.x + 10, width - tooltipWidth)
            val tooltipY = min(info.y - tooltipHeight - 10, height - tooltipHeight)

            g2d.color = Color(0, 0, 0, 200)
            g2d.fillRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)
            g2d.color = UiColors.defaultText
            g2d.drawRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)

            g2d.font = tooltipFont
            g2d.color = UiColors.teal
            g2d.drawString("Throughput", tooltipX + 5, tooltipY + 15)

            g2d.font = tooltipPlainFont
            g2d.color = UiColors.defaultText
            g2d.drawString(throughputText, tooltipX + 5, tooltipY + 30)
        }
    }

    override fun keyPressed(e: KeyEvent) {}
    override fun keyTyped(e: KeyEvent) {}
    override fun keyReleased(e: KeyEvent) {
        panel.repaint()
    }

    override fun mousePressed(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y

        timeRangeButtons.forEachIndexed { index, buttonRect ->
            if (buttonRect.contains(mouseposX, mouseposY)) {
                selectedTimeRange = TimeRange.entries[index]
                dashboardService.setSelectedTimeRange(selectedTimeRange.minutes) // normalized inside service
                panel.repaint()
                return
            }
        }
        panel.repaint()
    }

    override fun mouseClicked(e: MouseEvent) {}
    override fun mouseReleased(e: MouseEvent) {}
    override fun mouseEntered(e: MouseEvent) {
        if (!mouseInside) mouseInside = true
    }
    override fun mouseExited(e: MouseEvent) {
        if (mouseInside) mouseInside = false
        hoveredPodBar = null
        podTooltipInfo = null
        throughputTooltipInfo = null
    }

    override fun mouseWheelMoved(e: MouseWheelEvent) {}
    override fun mouseDragged(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y
        panel.repaint()
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y

        // Pod bar hover hit-testing
        val chartY = 330
        val chartHeight = 120
        val podThroughputData =
            if (selectedTimeRange == TimeRange.FIVE_MINUTES)
                dashboardService.getCurrentPodThroughput()
            else
                dashboardService.getPodThroughputRates(selectedTimeRange.minutes)

        if (podThroughputData.isNotEmpty() && mouseposY in chartY..(chartY + chartHeight)) {
            val maxThroughput = (podThroughputData.values.maxOrNull() ?: 0.0).coerceAtLeast(1.0)
            val barWidth = (width - 60) / podThroughputData.size.toFloat()
            val maxBarHeight = chartHeight - 60

            podThroughputData.entries.forEachIndexed { index, (podName, throughput) ->
                val barHeight = ((throughput / maxThroughput) * maxBarHeight).toInt().coerceAtLeast(0)
                val barX = 20 + (index * barWidth).toInt()
                val barY = chartY + chartHeight - 30 - barHeight

                val barRect = Rectangle(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)
                val labelRect = Rectangle(barX, chartY + chartHeight - 20, barWidth.toInt().coerceAtLeast(1), 15)
                if (barRect.contains(mouseposX, mouseposY) || labelRect.contains(mouseposX, mouseposY)) {
                    if (hoveredPodBar != podName) {
                        hoveredPodBar = podName
                        panel.repaint()
                    }
                    return
                }
            }
        }

        if (hoveredPodBar != null) {
            hoveredPodBar = null
            podTooltipInfo = null
            panel.repaint()
        }

        panel.repaint()
    }

    private fun refreshDashboardData() {
        // Trigger data collection refresh in the dashboard service
        // This will update all the metrics, throughput data, log level data, etc.
        try {
            // Force a refresh of the underlying data by triggering the data collector
            val dataCollector = Class.forName("widgets.DataCollector")
                .getDeclaredMethod("getInstance")
                .invoke(null) as? Any

            // Trigger a data refresh cycle
            dataCollector?.javaClass?.getDeclaredMethod("triggerRefresh")
                ?.invoke(dataCollector)
        } catch (e: Exception) {
            // If reflection fails, the data will still be refreshed through normal channels
            // as the dashboard service callback will handle UI updates
        }

        // Also refresh Kafka lag information specifically
        refreshKafkaLagData()

        // The UI will be repainted automatically through the callback system
        // when new data becomes available
    }

    private fun refreshKafkaLagData() {
        try {
            // Use the same mechanism as KafkaLagView to refresh lag data
            val kafkaChannel = Class.forName("app.Channels")
                .getDeclaredField("kafkaChannel")
                .get(null) as? kotlinx.coroutines.channels.Channel<Any>

            val listLagClass = Class.forName("kafka.Kafka\$ListLag")
            val listLagConstructor = listLagClass.getDeclaredConstructor()
            val listLagInstance = listLagConstructor.newInstance()

            // Send the ListLag message to trigger lag data collection
            kafkaChannel?.let { channel ->
                // Use a separate thread to send the message since we're not in a coroutine context
                Thread {
                    try {
                        runBlocking {
                            channel.send(listLagInstance)
                        }
                    } catch (e: Exception) {
                        // Ignore send errors
                    }
                }.start()
            }
        } catch (e: Exception) {
            // If Kafka lag refresh fails, continue silently - lag data might not be available
            // but other dashboard data will still be refreshed
        }
    }

    private fun getLevelColor(level: LogLevel): Color =
        when (level) {
            LogLevel.DEBUG -> UiColors.defaultText
            LogLevel.INFO -> UiColors.green
            LogLevel.WARN -> UiColors.orange
            LogLevel.ERROR -> UiColors.red
            else -> UiColors.defaultText
        }

    private fun Long.format(): String =
        when {
            this >= 1_000_000_000 -> String.format("%.1fB", this / 1_000_000_000.0)
            this >= 1_000_000 -> String.format("%.1fM", this / 1_000_000.0)
            this >= 1_000 -> String.format("%.1fK", this / 1_000.0)
            else -> this.toString()
        }
}