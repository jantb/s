package widgets

import ComponentOwn
import LogLevel
import SlidePanel
import State
import util.Styles
import util.UiColors
import java.awt.*
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import java.util.concurrent.atomic.AtomicReference
import javax.swing.SwingUtilities
import kotlin.math.min

class DashboardView(private val panel: SlidePanel, x: Int, y: Int, width: Int, height: Int) : ComponentOwn(),
    KeyListener,
    MouseListener,
    MouseWheelListener,
    MouseMotionListener {

    private val dashboardService = DashboardService.getInstance()
    private val metrics = AtomicReference<DashboardService.DashboardMetrics>()
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
    private val buttonFont = Font(Styles.normalFont, Font.PLAIN, 11)
    private val tooltipFont = Font(Styles.normalFont, Font.BOLD, 12)
    private val tooltipPlainFont = Font(Styles.normalFont, Font.PLAIN, 11)
    private val metricFont = Font(Styles.normalFont, Font.BOLD, 16)
    private val valueFont = Font(Styles.normalFont, Font.PLAIN, 14)

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

        // Set up instant update callback
        dashboardService.setOnDataUpdateCallback {
            SwingUtilities.invokeLater {
                panel.repaint()
            }
        }

        // Initialize the selected time range in the service
        dashboardService.setSelectedTimeRange(selectedTimeRange.minutes)

        // Thread for periodic updates as backup
        val metricsThread = Thread {
            while (true) {
                try {
                    Thread.sleep(1000) // Update every second as backup
                    SwingUtilities.invokeLater {
                        panel.repaint()
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }
        }
        metricsThread.isDaemon = true
        metricsThread.start()
    }

    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        if (this.width != width || this.height != height || this.x != x || this.y != y) {
            try {
                if (::g2d.isInitialized) {
                    g2d.dispose()
                }
            } catch (e: Exception) {
                // g2d not initialized yet, which is fine
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

        // Ensure g2d is initialized before using it
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
        // Not implemented
    }

    private fun paint() {
        g2d.color = UiColors.defaultText

        // Calculate chart height and visible lines
        chartHeight = 200
        val charHeight = maxCharBounds?.height?.toInt() ?: 12
        visibleLines = ((height - chartHeight - 30) / charHeight) - 2

        // Draw time range selection buttons
        drawTimeRangeButtons()

        // Draw main metrics cards
        drawMetricsCards()

        // Draw throughput chart
        drawThroughputChart()

        // Draw log level distribution chart
        drawLogLevelChart()

        // Draw pod throughput chart
        drawPodThroughputChart()

        // Draw Kafka lag information
        drawKafkaLagInfo()

        // Draw tooltip if needed
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

            // Button background
            val isSelected = timeRange == selectedTimeRange
            g2d.color = if (isSelected) UiColors.selection else Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 150)
            g2d.fillRoundRect(buttonX, buttonY, buttonWidth, buttonHeight, 4, 4)

            // Button border
            g2d.color = if (isSelected) UiColors.selection.darker() else UiColors.defaultText.darker()
            g2d.drawRoundRect(buttonX, buttonY, buttonWidth, buttonHeight, 4, 4)

            // Button text
            g2d.font = Font(Styles.normalFont, Font.BOLD, 10)
            g2d.color = if (isSelected) UiColors.defaultText else UiColors.defaultText.darker()
            g2d.drawString(timeRange.displayName, buttonX + 5, buttonY + 14)

            buttonX += buttonWidth + buttonSpacing
        }
    }

    private fun drawMetricsCards() {
        val currentMetrics = dashboardService.getDashboardMetrics()

        // Main metrics cards at the top
        val cardWidth = 200
        val cardHeight = 80
        val cardSpacing = 20
        val startY = 35  // Leave space for time range buttons

        val metrics = listOf(
            Triple("Total Messages", currentMetrics.totalMessages.format(), UiColors.teal),
            Triple("Avg Throughput", String.format("%.1f/s", currentMetrics.averageThroughput), UiColors.green),
            Triple("Error Rate", String.format("%.1f%%", currentMetrics.errorRate), UiColors.red),
            Triple("Active Pods", currentMetrics.activePods.toString(), UiColors.magenta)
        )

        metrics.forEachIndexed { index, (label, value, color) ->
            val cardX = 10 + index * (cardWidth + cardSpacing)

            // Card background
            g2d.color = Color(color.red, color.green, color.blue, 30)
            g2d.fillRoundRect(cardX, startY, cardWidth, cardHeight, 10, 10)

            // Card border
            g2d.color = color.darker()
            g2d.drawRoundRect(cardX, startY, cardWidth, cardHeight, 10, 10)

            // Label
            g2d.font = headerPlainFont
            g2d.color = UiColors.defaultText
            g2d.drawString(label, cardX + 10, startY + 20)

            // Value
            g2d.font = metricFont
            g2d.color = color
            g2d.drawString(value, cardX + 10, startY + 50)

            // Small trend indicator (placeholder)
            g2d.font = Font(Styles.normalFont, Font.BOLD, 10)
            g2d.color = UiColors.green
            g2d.drawString("↗", cardX + cardWidth - 20, startY + 20)
        }
    }

    private fun drawThroughputChart() {
        val chartY = 130  // Moved down to account for time range buttons
        val chartHeight = 80

        // Chart background
        g2d.color = Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200)
        g2d.fillRect(10, chartY, width - 20, chartHeight)

        // Chart title
        g2d.font = headerBoldFont
        g2d.color = UiColors.defaultText.brighter()
        g2d.drawString("Throughput (Last ${selectedTimeRange.displayName})", 20, chartY + 20)

        // Get throughput data with appropriate resolution based on time range
        val throughputData = when (selectedTimeRange) {
            TimeRange.FIVE_MINUTES -> dashboardService.getHighResThroughputData(5 * 60) // 5 minutes of high-res data
            TimeRange.FIFTEEN_MINUTES -> dashboardService.getTimeBucketedThroughputData(15, 30) // 30-second buckets
            TimeRange.ONE_HOUR -> dashboardService.getTimeBucketedThroughputData(60, 60) // 1-minute buckets
            TimeRange.SIX_HOURS -> dashboardService.getTimeBucketedThroughputData(360, 300) // 5-minute buckets
            TimeRange.TWENTY_FOUR_HOURS -> dashboardService.getTimeBucketedThroughputData(1440, 600) // 10-minute buckets
        }

        if (throughputData.isNotEmpty()) {
            // Calculate throughput rates properly by looking at differences between consecutive data points
            val throughputRates = mutableListOf<Double>()

            for (i in 1 until throughputData.size) {
                val current = throughputData[i]
                val previous = throughputData[i - 1]

                val currentTotal = current.podThroughput.values.sum() + current.queueThroughput.values.sum()
                val previousTotal = previous.podThroughput.values.sum() + previous.queueThroughput.values.sum()
                val messageDiff = currentTotal - previousTotal

                val timeDiffSeconds = (current.messageTimestamp - previous.messageTimestamp).inWholeSeconds
                val rate = if (timeDiffSeconds > 0) messageDiff.toDouble() / timeDiffSeconds else 0.0

                throughputRates.add(rate)
            }

            if (throughputRates.isNotEmpty()) {
                val maxThroughput = throughputRates.maxOrNull() ?: 1.0
                val chartWidth = width - 40
                val barWidth = chartWidth / throughputRates.size.toFloat()

                throughputRates.forEachIndexed { index, rate ->
                    val barHeight = if (maxThroughput > 0) (rate / maxThroughput * (chartHeight - 40)).toInt() else 0

                    val barX = 20 + (index * barWidth).toInt()
                    val barY = chartY + chartHeight - 20 - barHeight

                    // Check if mouse is hovering over this bar
                    val barRect = Rectangle(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)
                    if (barRect.contains(mouseposX, mouseposY)) {
                        // Highlight the bar
                        g2d.color = UiColors.teal.brighter()
                        // Set tooltip info
                        throughputTooltipInfo = ThroughputTooltipInfo(
                            x = mouseposX,
                            y = mouseposY,
                            throughput = rate,
                            timestamp = throughputData[index + 1].messageTimestamp.toString()
                        )
                    } else {
                        // Normal bar color
                        g2d.color = UiColors.teal
                    }

                    g2d.fillRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)

                    // Bar border
                    g2d.color = UiColors.teal.darker()
                    g2d.drawRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)
                }
            }
        } else {
            // No data message
            g2d.font = headerPlainFont
            g2d.color = UiColors.defaultText.darker()
            g2d.drawString("No throughput data available", 20, chartY + 40)
        }

        // Chart border
        g2d.color = UiColors.defaultText.darker()
        g2d.drawRect(10, chartY, width - 20, chartHeight)
    }

    private fun drawLogLevelChart() {
        val chartY = 230  // Moved down to account for time range buttons
        val chartHeight = 80

        // Chart background
        g2d.color = Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200)
        g2d.fillRect(10, chartY, width - 20, chartHeight)

        // Chart title
        g2d.font = headerBoldFont
        g2d.color = UiColors.defaultText.brighter()
        g2d.drawString("Log Level Distribution", 20, chartY + 20)

        // Get log level data with appropriate resolution based on time range
        val logLevelData = when (selectedTimeRange) {
            TimeRange.FIVE_MINUTES -> dashboardService.getLogLevelData(5)
            TimeRange.FIFTEEN_MINUTES -> dashboardService.getLogLevelData(15)
            TimeRange.ONE_HOUR -> dashboardService.getLogLevelData(60)
            TimeRange.SIX_HOURS -> dashboardService.getLogLevelData(360)
            TimeRange.TWENTY_FOUR_HOURS -> dashboardService.getLogLevelData(1440)
        }

        if (logLevelData.isNotEmpty()) {
            val selectedLevels = State.levels.get()
            val levelCounts = mutableMapOf<LogLevel, Long>()

            logLevelData.forEach { dataPoint ->
                dataPoint.counts.forEach { (level, count) ->
                    if (level in selectedLevels) {
                        levelCounts[level] = levelCounts.getOrDefault(level, 0L) + count
                    }
                }
            }

            val total = levelCounts.values.sum()
            val barWidth = if (selectedLevels.isNotEmpty()) (width - 40) / selectedLevels.size.toFloat() else 0f

            selectedLevels.forEachIndexed { index, level ->
                val count = levelCounts[level] ?: 0L
                val percentage = if (total > 0) (count.toDouble() / total * 100) else 0.0
                val barHeight = if (total > 0) (count.toDouble() / levelCounts.values.max() * (chartHeight - 40)).toInt() else 0

                val barX = 20 + (index * barWidth).toInt()
                val barY = chartY + chartHeight - 20 - barHeight

                // Bar color based on level
                g2d.color = getLevelColor(level)
                g2d.fillRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)

                // Bar border
                g2d.color = getLevelColor(level).darker()
                g2d.drawRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)

                // Level label
                g2d.font = Font(Styles.normalFont, Font.PLAIN, 10)
                g2d.color = UiColors.defaultText
                g2d.drawString(level.name, barX + 2, chartY + chartHeight - 5)
            }
        } else {
            // No data message
            g2d.font = headerPlainFont
            g2d.color = UiColors.defaultText.darker()
            g2d.drawString("No log level data available", 20, chartY + 40)
        }

        // Chart border
        g2d.color = UiColors.defaultText.darker()
        g2d.drawRect(10, chartY, width - 20, chartHeight)
    }

    private fun drawPodThroughputChart() {
        val chartY = 330  // Position after log level chart
        val chartHeight = 120

        // Chart background
        g2d.color = Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200)
        g2d.fillRect(10, chartY, width - 20, chartHeight)

        // Chart title
        g2d.font = headerBoldFont
        g2d.color = UiColors.defaultText.brighter()
        g2d.drawString("Pod Throughput (Last ${selectedTimeRange.displayName})", 20, chartY + 20)

        // Get pod throughput data with high resolution
        val rawPodThroughputData = when (selectedTimeRange) {
            TimeRange.FIVE_MINUTES -> dashboardService.getCurrentPodThroughput() // Real-time data
            else -> dashboardService.getPodThroughputRates(selectedTimeRange.minutes) // Proper rate calculation
        }
        val activePods = dashboardService.getActivePods()

        // Filter out empty pod names and queue throughput (which might be mixed in)
        val podThroughputData = rawPodThroughputData.filter { (podName, _) ->
            podName.isNotBlank() && !podName.startsWith("queue_")
        }

        if (podThroughputData.isNotEmpty()) {
            val maxThroughput = podThroughputData.values.maxOrNull() ?: 1.0
            val barWidth = (width - 60) / podThroughputData.size.toFloat()
            val maxBarHeight = chartHeight - 60

            podThroughputData.entries.forEachIndexed { index, (podName, throughput) ->
                val barHeight = if (maxThroughput > 0) (throughput / maxThroughput * maxBarHeight).toInt() else 0
                val barX = 20 + (index * barWidth).toInt()
                val barY = chartY + chartHeight - 30 - barHeight

                // Determine bar color based on whether pod is active
                val isActive = activePods.contains(podName)
                g2d.color = if (isActive) UiColors.green else UiColors.teal

                // Check if mouse is hovering over this bar
                val barRect = Rectangle(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)
                if (hoveredPodBar == podName) {
                    g2d.color = g2d.color.brighter()
                    // Set tooltip info
                    podTooltipInfo = PodTooltipInfo(
                        x = mouseposX,
                        y = mouseposY,
                        podName = podName,
                        throughput = throughput,
                        isActive = isActive
                    )
                }

                g2d.fillRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)

                // Bar border
                g2d.color = g2d.color.darker()
                g2d.drawRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)

                // Pod name label (truncated if too long, but show full name on hover)
                g2d.font = Font(Styles.normalFont, Font.PLAIN, 9)
                g2d.color = UiColors.defaultText
                // Calculate how much space is available for the label
                val availableWidth = barWidth.toInt().coerceAtLeast(1) - 4 // Leave 2px padding on each side
                val maxChars = (availableWidth / g2d.fontMetrics.charWidth('A')).coerceAtMost(podName.length)
                val displayName = if (podName.length > maxChars) podName.take(maxChars - 3) + "..." else podName
                g2d.drawString(displayName, barX + 2, chartY + chartHeight - 10)

                // Show full pod name in tooltip if hovering over label area
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

                // Throughput value on top of bar
                if (barHeight > 15) {
                    g2d.font = Font(Styles.normalFont, Font.BOLD, 8)
                    g2d.color = UiColors.defaultText
                    g2d.drawString(String.format("%.1f", throughput), barX + 2, barY - 2)
                }
            }
        } else {
            // No data message
            g2d.font = headerPlainFont
            g2d.color = UiColors.defaultText.darker()
            g2d.drawString("No pod throughput data available", 20, chartY + 40)
        }

        // Chart border
        g2d.color = UiColors.defaultText.darker()
        g2d.drawRect(10, chartY, width - 20, chartHeight)
    }

    private fun drawKafkaLagInfo() {
        val lagY = 470  // Moved down to account for pod throughput chart
        val lagHeight = height - lagY - 10

        // Background
        g2d.color = Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200)
        g2d.fillRect(10, lagY, width - 20, lagHeight)

        // Title
        g2d.font = headerBoldFont
        g2d.color = UiColors.defaultText.brighter()
        g2d.drawString("Kafka Lag Information", 20, lagY + 20)

        val lagSummary = dashboardService.getKafkaLagSummary()
        val partitionDetails = dashboardService.getPartitionLagDetails()

        if (partitionDetails.isNotEmpty()) {
            // Draw lag summary
            g2d.font = headerPlainFont
            g2d.color = UiColors.teal
            g2d.drawString("High Lag: ${lagSummary["high"] ?: 0}", 20, lagY + 40)
            g2d.color = UiColors.orange
            g2d.drawString("Medium Lag: ${lagSummary["medium"] ?: 0}", 20, lagY + 55)
            g2d.color = UiColors.green
            g2d.drawString("Low Lag: ${lagSummary["low"] ?: 0}", 20, lagY + 70)

            // Draw partition details table
            g2d.font = Font(Styles.normalFont, Font.BOLD, 12)
            g2d.color = UiColors.magenta
            g2d.drawString("Topic", 20, lagY + 95)
            g2d.drawString("Partition", 150, lagY + 95)
            g2d.drawString("Lag", 220, lagY + 95)
            g2d.drawString("Group", 280, lagY + 95)

            // Draw separator line
            g2d.color = UiColors.defaultText.darker()
            g2d.drawLine(20, lagY + 100, width - 30, lagY + 100)

            // Draw partition details
            g2d.font = Font(Styles.normalFont, Font.PLAIN, 11)
            val maxVisibleItems = minOf(20, partitionDetails.size) // Show max 20 items

            for (i in 0 until maxVisibleItems) {
                val partition = partitionDetails[i]
                val y = lagY + 115 + i * 15

                g2d.color = UiColors.defaultText
                g2d.drawString(partition["topic"] as String, 20, y)
                g2d.drawString((partition["partition"] as Int).toString(), 150, y)

                // Color lag based on severity
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
            // No lag data message
            g2d.font = headerPlainFont
            g2d.color = UiColors.defaultText.darker()
            g2d.drawString("No Kafka lag data available", 20, lagY + 40)
        }

        // Border
        g2d.color = UiColors.defaultText.darker()
        g2d.drawRect(10, lagY, width - 20, lagHeight)
    }

    // Removed unused drawSelectedLine method

    private fun drawTooltip() {
        // Draw log level tooltip if present
        tooltipInfo?.let { info ->
            // Calculate tooltip dimensions based on text content
            g2d.font = tooltipFont
            val titleWidth = g2d.fontMetrics.stringWidth(info.level.name)
            g2d.font = tooltipPlainFont
            val countText = "Count: ${info.count}"
            val shareText = "Share: ${String.format("%.1f%%", info.percentage)}"
            val countWidth = g2d.fontMetrics.stringWidth(countText)
            val shareWidth = g2d.fontMetrics.stringWidth(shareText)
            val maxTextWidth = maxOf(titleWidth, countWidth, shareWidth)

            val tooltipWidth = maxOf(120, maxTextWidth + 20)
            val tooltipHeight = 50

            // Ensure tooltip doesn't go off screen
            val tooltipX = min(info.x + 10, width - tooltipWidth)
            val tooltipY = min(info.y - tooltipHeight - 10, height - tooltipHeight)

            // Tooltip background
            g2d.color = Color(0, 0, 0, 200)
            g2d.fillRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)

            // Tooltip border
            g2d.color = UiColors.defaultText
            g2d.drawRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)

            // Tooltip text
            g2d.font = tooltipFont
            g2d.color = getLevelColor(info.level)
            g2d.drawString(info.level.name, tooltipX + 5, tooltipY + 15)

            g2d.font = tooltipPlainFont
            g2d.color = UiColors.defaultText
            g2d.drawString(countText, tooltipX + 5, tooltipY + 30)
            g2d.drawString(shareText, tooltipX + 5, tooltipY + 42)
        }

        // Draw pod tooltip if present
        podTooltipInfo?.let { info ->
            // Calculate tooltip dimensions based on text content
            g2d.font = tooltipFont
            val podNameWidth = g2d.fontMetrics.stringWidth(info.podName)
            g2d.font = tooltipPlainFont
            val throughputText = "Throughput: ${String.format("%.1f/s", info.throughput)}"
            val statusText = "Status: ${if (info.isActive) "Active" else "Inactive"}"
            val throughputWidth = g2d.fontMetrics.stringWidth(throughputText)
            val statusWidth = g2d.fontMetrics.stringWidth(statusText)
            val maxTextWidth = maxOf(podNameWidth, throughputWidth, statusWidth)

            val tooltipWidth = maxOf(140, maxTextWidth + 20)
            val tooltipHeight = 55

            // Ensure tooltip doesn't go off screen
            val tooltipX = min(info.x + 10, width - tooltipWidth)
            val tooltipY = min(info.y - tooltipHeight - 10, height - tooltipHeight)

            // Tooltip background
            g2d.color = Color(0, 0, 0, 200)
            g2d.fillRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)

            // Tooltip border
            g2d.color = UiColors.defaultText
            g2d.drawRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)

            // Tooltip text
            g2d.font = tooltipFont
            g2d.color = if (info.isActive) UiColors.green else UiColors.teal
            g2d.drawString(info.podName, tooltipX + 5, tooltipY + 15)

            g2d.font = tooltipPlainFont
            g2d.color = UiColors.defaultText
            g2d.drawString(throughputText, tooltipX + 5, tooltipY + 30)
            g2d.drawString(statusText, tooltipX + 5, tooltipY + 45)
        }

        // Draw throughput tooltip if present
        throughputTooltipInfo?.let { info ->
            // Calculate tooltip dimensions based on text content
            g2d.font = tooltipFont
            val titleWidth = g2d.fontMetrics.stringWidth("Throughput")
            g2d.font = tooltipPlainFont
            val throughputText = String.format("%.1f/s", info.throughput)
            val throughputWidth = g2d.fontMetrics.stringWidth(throughputText)
            val maxTextWidth = maxOf(titleWidth, throughputWidth)

            val tooltipWidth = maxOf(100, maxTextWidth + 20)
            val tooltipHeight = 40

            // Ensure tooltip doesn't go off screen
            val tooltipX = min(info.x + 10, width - tooltipWidth)
            val tooltipY = min(info.y - tooltipHeight - 10, height - tooltipHeight)

            // Tooltip background
            g2d.color = Color(0, 0, 0, 200)
            g2d.fillRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)

            // Tooltip border
            g2d.color = UiColors.defaultText
            g2d.drawRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight)

            // Tooltip text
            g2d.font = tooltipFont
            g2d.color = UiColors.teal
            g2d.drawString("Throughput", tooltipX + 5, tooltipY + 15)

            g2d.font = tooltipPlainFont
            g2d.color = UiColors.defaultText
            g2d.drawString(throughputText, tooltipX + 5, tooltipY + 30)
        }
    }

    override fun keyPressed(e: KeyEvent) {
        // Dashboard view doesn't need keyboard navigation
    }

    override fun keyTyped(e: KeyEvent) {}
    override fun keyReleased(e: KeyEvent) {
        panel.repaint()
    }

    override fun mousePressed(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y

        // Check if time range button was clicked
        timeRangeButtons.forEachIndexed { index, buttonRect ->
            if (buttonRect.contains(mouseposX, mouseposY)) {
                selectedTimeRange = TimeRange.entries[index]
                // Update the dashboard service with the new time range
                dashboardService.setSelectedTimeRange(selectedTimeRange.minutes)
                panel.repaint()
                return
            }
        }

        panel.repaint()
    }

    override fun mouseClicked(e: MouseEvent) {}
    override fun mouseReleased(e: MouseEvent) {}
    override fun mouseEntered(e: MouseEvent) {
        if (!mouseInside) {
            mouseInside = true
        }
    }

    override fun mouseExited(e: MouseEvent) {
        if (mouseInside) {
            mouseInside = false
        }
        // Clear pod hover state when mouse leaves
        hoveredPodBar = null
        podTooltipInfo = null
        // Clear throughput hover state when mouse leaves
        throughputTooltipInfo = null
    }

    override fun mouseWheelMoved(e: MouseWheelEvent) {
        // Dashboard view doesn't need mouse wheel navigation
    }

    override fun mouseDragged(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y
        panel.repaint()
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y

        // Check for pod bar hover
        val chartY = 330
        val chartHeight = 120
        val podThroughputData = when (selectedTimeRange) {
            TimeRange.FIVE_MINUTES -> dashboardService.getCurrentPodThroughput()
            else -> dashboardService.getPodThroughputRates(selectedTimeRange.minutes)
        }

        if (podThroughputData.isNotEmpty() && mouseposY in chartY..(chartY + chartHeight)) {
            val maxThroughput = podThroughputData.values.maxOrNull() ?: 1.0
            val barWidth = (width - 60) / podThroughputData.size.toFloat()
            val maxBarHeight = chartHeight - 60

            podThroughputData.entries.forEachIndexed { index, (podName, throughput) ->
                val barHeight = if (maxThroughput > 0) (throughput / maxThroughput * maxBarHeight).toInt() else 0
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

        // Clear pod hover if not over any bar or label
        if (hoveredPodBar != null) {
            hoveredPodBar = null
            podTooltipInfo = null
            panel.repaint()
        }

        panel.repaint()
    }

    private fun Long.format(): String {
        return when {
            this >= 1_000_000_000 -> String.format("%.1fB", this / 1_000_000_000.0)
            this >= 1_000_000 -> String.format("%.1fM", this / 1_000_000.0)
            this >= 1_000 -> String.format("%.1fK", this / 1_000.0)
            else -> this.toString()
        }
    }
}