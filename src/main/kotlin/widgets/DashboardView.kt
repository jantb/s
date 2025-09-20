package widgets

import ComponentOwn
import LogLevel
import SlidePanel
import State
import app.Channels
import app.QueryChanged
import util.Styles
import util.UiColors
import widgets.getLevelColor
import java.awt.*
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import java.util.concurrent.atomic.AtomicReference
import javax.swing.SwingUtilities
import kotlin.math.max
import kotlin.math.min
import kotlin.time.Duration.Companion.seconds

class DashboardView(private val panel: SlidePanel, x: Int, y: Int, width: Int, height: Int) : ComponentOwn(),
    KeyListener,
    MouseListener,
    MouseWheelListener,
    MouseMotionListener {

    private val dashboardService = DashboardService.getInstance()
    private val metrics = AtomicReference<DashboardService.DashboardMetrics>()
    private var indexOffset = 0
    private var visibleLines = 0
    private var selectedLineIndex = 0
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
    private var hoveredBar: Pair<Int, LogLevel>? = null
    private var tooltipInfo: TooltipInfo? = null

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

        // Thread for updating metrics periodically
        val metricsThread = Thread {
            while (true) {
                try {
                    Thread.sleep(1000) // Update every second
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
        drawSelectedLine()
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

        // Get throughput data
        val throughputData = dashboardService.getThroughputData(selectedTimeRange.minutes)

        if (throughputData.isNotEmpty()) {
            val maxThroughput = throughputData.maxOf { it.podThroughput.values.sum() + it.queueThroughput.values.sum() }.toDouble()
            val chartWidth = width - 40
            val barWidth = chartWidth / throughputData.size.toFloat()

            throughputData.forEachIndexed { index, dataPoint ->
                val totalThroughput = dataPoint.podThroughput.values.sum() + dataPoint.queueThroughput.values.sum()
                val barHeight = if (maxThroughput > 0) (totalThroughput.toDouble() / maxThroughput * (chartHeight - 40)).toInt() else 0

                val barX = 20 + (index * barWidth).toInt()
                val barY = chartY + chartHeight - 20 - barHeight

                // Bar color
                g2d.color = UiColors.teal
                g2d.fillRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)

                // Bar border
                g2d.color = UiColors.teal.darker()
                g2d.drawRect(barX, barY, barWidth.toInt().coerceAtLeast(1), barHeight)
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

        // Get log level data
        val logLevelData = dashboardService.getLogLevelData(selectedTimeRange.minutes)

        if (logLevelData.isNotEmpty()) {
            val allLevels = LogLevel.entries.toTypedArray()
            val levelCounts = mutableMapOf<LogLevel, Long>()

            logLevelData.forEach { dataPoint ->
                dataPoint.counts.forEach { (level, count) ->
                    levelCounts[level] = levelCounts.getOrDefault(level, 0L) + count
                }
            }

            val total = levelCounts.values.sum()
            val barWidth = (width - 40) / allLevels.size.toFloat()

            allLevels.forEachIndexed { index, level ->
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

    private fun drawKafkaLagInfo() {
        val lagY = 330  // Moved down to account for time range buttons
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
            val startIndex = indexOffset.coerceAtMost(maxOf(0, partitionDetails.size - 1))
            val endIndex = minOf(startIndex + visibleLines, partitionDetails.size)

            for (i in startIndex until endIndex) {
                val partition = partitionDetails[i]
                val y = lagY + 115 + (i - startIndex) * 15

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

    private fun drawSelectedLine() {
        // Not used in dashboard view
    }

    private fun drawTooltip() {
        // Not used in dashboard view
    }

    override fun keyPressed(e: KeyEvent) {
        when (e.keyCode) {
            KeyEvent.VK_DOWN -> {
                indexOffset++
                panel.repaint()
            }
            KeyEvent.VK_UP -> {
                indexOffset--
                indexOffset = indexOffset.coerceAtLeast(0)
                panel.repaint()
            }
            KeyEvent.VK_PAGE_DOWN -> {
                indexOffset += visibleLines
                panel.repaint()
            }
            KeyEvent.VK_PAGE_UP -> {
                indexOffset -= visibleLines
                indexOffset = indexOffset.coerceAtLeast(0)
                panel.repaint()
            }
            KeyEvent.VK_HOME -> {
                indexOffset = 0
                panel.repaint()
            }
            KeyEvent.VK_END -> {
                indexOffset = Int.MAX_VALUE
                panel.repaint()
            }
        }
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
    }

    override fun mouseWheelMoved(e: MouseWheelEvent) {
        if ((e.isControlDown && !State.onMac) || (e.isMetaDown && State.onMac)) {
            rowHeight += e.wheelRotation
            rowHeight = rowHeight.coerceIn(1..100)
            panel.repaint()
            return
        }

        indexOffset += e.wheelRotation * e.scrollAmount
        indexOffset = indexOffset.coerceAtLeast(0)
        panel.repaint()
    }

    override fun mouseDragged(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y
        panel.repaint()
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y
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