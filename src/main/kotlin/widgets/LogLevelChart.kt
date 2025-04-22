package widgets

import ComponentOwn
import SlidePanel
import app.Domain
import app.QueryChanged
import app.Channels
import util.UiColors
import java.awt.BasicStroke
import java.awt.Color
import java.awt.Font
import java.awt.Graphics2D
import java.awt.Rectangle
import java.awt.event.KeyEvent
import java.awt.event.MouseEvent
import java.awt.event.MouseWheelEvent
import java.awt.image.BufferedImage
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit

/**
 * A component that displays a bar chart of log levels.
 * The chart counts the number of log entries for each log level (INFO, WARN, DEBUG, ERROR)
 * and displays them as color-coded bars on a time-based x-axis.
 */
class LogLevelChart(
    private val panel: SlidePanel,
    x: Int,
    y: Int,
    width: Int,
    height: Int,
    private val onLevelsChanged: (() -> Unit)? = null
) : ComponentOwn() {

    // Data structure to store log counts by time period and level
    private data class TimePoint(val time: Instant, val counts: MutableMap<String, Int> = mutableMapOf())

    // List of time points for the chart
    private val timePoints = mutableListOf<TimePoint>()

    // Time range for the chart
    private var startTime: Instant = Instant.now()
    private var endTime: Instant = Instant.now()

    // Number of time divisions to display
    private val numTimeDivisions = 250

    // Set of all possible log levels
    private val allLogLevels = mutableSetOf("INFO", "WARN", "DEBUG", "ERROR", "UNKNOWN")

    // Set of currently selected log levels (all selected by default)
    private val selectedLevels = mutableSetOf("INFO", "WARN", "DEBUG", "ERROR", "UNKNOWN")

    // Rectangles for level labels (used for click detection)
    private val levelRectangles = mutableMapOf<String, Rectangle>()

    init {
        this.x = x
        this.y = y
        this.height = height
        this.width = width
    }

    private lateinit var image: BufferedImage
    private lateinit var g2d: Graphics2D

    /**
     * Update the chart with new log data.
     * This method groups logs by time periods and counts the number of entries for each log level.
     * Only the last 100,000 messages are used to keep the chart fast and optimized.
     */
    fun updateChart(logs: List<Domain>) {
        // Update the set of all log levels from the logs
        logs.forEach { domain ->
            val level = domain.level.ifEmpty { "UNKNOWN" }
            allLogLevels.add(level)
        }

        if (logs.isEmpty()) {
            timePoints.clear()
            panel.repaint()
            return
        }

        // Limit to the last 100,000 logs for performance
        val limitedLogs = if (logs.size > 10_000) logs.takeLast(10_000) else logs

        // Determine time range from logs
        startTime = limitedLogs.minByOrNull { it.timestamp }?.timestamp ?: Instant.now()
        endTime = limitedLogs.maxByOrNull { it.timestamp }?.timestamp ?: Instant.now()

        // Ensure we have a valid time range
        if (startTime == endTime) {
            endTime = startTime.plus(1, ChronoUnit.MINUTES)
        }

        // Calculate time interval for divisions
        val timeInterval = Duration.between(startTime, endTime).dividedBy(numTimeDivisions.toLong())

        // Create time points
        timePoints.clear()
        for (i in 0 until numTimeDivisions) {
            val pointTime = startTime.plus(timeInterval.multipliedBy(i.toLong()))
            timePoints.add(TimePoint(pointTime))
        }

        // Count logs by time period and level
        limitedLogs.forEach { domain ->
            val level = domain.level.ifEmpty { "UNKNOWN" }

            // Find the appropriate time point
            val timePoint = timePoints.findLast { it.time.isBefore(domain.timestamp) || it.time == domain.timestamp }
                ?: timePoints.firstOrNull()

            timePoint?.let {
                it.counts[level] = it.counts.getOrDefault(level, 0) + 1
            }
        }

        // Repaint the chart
        panel.repaint()
    }

    /**
     * Get the currently selected log levels
     */
    fun getSelectedLevels(): Set<String> {
        return selectedLevels.toSet()
    }

    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        // Always update dimensions to ensure the chart resizes with the window
        if (this.width != width || this.height != height || this.x != x || this.y != y || !::image.isInitialized) {
            if (::g2d.isInitialized) {
                this.g2d.dispose()
            }
            this.image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
            this.g2d = this.image.createGraphics()
            this.height = height
            this.width = width
            this.x = x
            this.y = y
        }

        // Clear background
        g2d.color = UiColors.background
        g2d.fillRect(0, 0, width, height)

        // Draw chart
        drawChart()

        return image
    }

    override fun repaint(componentOwn: ComponentOwn) {
        // Not implemented
    }

    private fun drawChart() {
        // Draw an empty chart with axes and gridlines if no data
        if (timePoints.isEmpty()) {
            drawEmptyChart()
            return
        }

        // Get all log levels (use the predefined set)
        val sortedLevels = allLogLevels.toList().sorted()

        // Calculate the maximum total count for scaling (for stacked bars)
        var maxTotalCount = 0
        val timePointsCopy = ArrayList(timePoints)
        timePointsCopy.forEach { timePoint ->
            val totalCount = timePoint.counts.values.sum()
            if (totalCount > maxTotalCount) maxTotalCount = totalCount
        }

        // Calculate bar width based on number of time points
        val timeSlotWidth = (width - 60) / timePointsCopy.size.coerceAtLeast(1)
        val barWidth = timeSlotWidth - 2 // Full width with small spacing

        // Draw gridlines
        drawGridlines(maxTotalCount)

        // Draw time axis
        g2d.color = Color.GRAY // Use gray color for time axis
        g2d.drawLine(30, height - 35, width - 30, height - 35)

        // Draw stacked bars for each time point
        timePointsCopy.forEachIndexed { timeIndex, timePoint ->
            val xPosBase = 30 + (timeIndex * timeSlotWidth)

            // Draw time label (only for some time points to avoid crowding)
            if (timeIndex % 25 == 0 || timeIndex == timePointsCopy.size - 1) {
                val formatter = DateTimeFormatter.ofPattern("HH:mm:ss")
                    .withZone(ZoneId.systemDefault())
                val timeLabel = formatter.format(timePoint.time)
                g2d.font = Font("Monospaced", Font.PLAIN, 8)
                g2d.color = Color.GRAY // Use gray color for x-axis dates
                g2d.drawString(timeLabel, xPosBase, height - 15)
            }

            // Draw stacked bars for each log level at this time point
            var currentHeight = 0
            val countsCopy = HashMap(timePoint.counts)

            // Draw bars in a consistent order
            sortedLevels.forEach { level ->
                // Only draw bars for selected levels
                if (level in selectedLevels) {
                    val count = countsCopy.getOrDefault(level, 0)
                    if (count > 0) {
                        // Calculate bar height (scaled to fit in the chart)
                        val barHeight = ((count.toFloat() / maxTotalCount) * (height - 80)).toInt().coerceAtLeast(1)

                        // Set bar color based on log level
                        g2d.color = getLevelColor(level)

                        // Draw the bar (stacked on top of previous bars)
                        g2d.fillRect(xPosBase, height - 40 - currentHeight - barHeight, barWidth, barHeight)

                        // Update the current height for the next bar in the stack
                        currentHeight += barHeight
                    }
                }
            }
        }

        // Draw legend for log levels (all levels, not just those in the data)
        drawLegend(sortedLevels)
    }

    /**
     * Draw an empty chart with axes and gridlines
     */
    private fun drawEmptyChart() {
        // Draw background
        g2d.color = UiColors.background
        g2d.fillRect(0, 0, width, height)

        // Draw axes
        g2d.color = Color.GRAY // Use gray color for axes
        g2d.drawLine(30, height - 35, width - 30, height - 35) // X-axis
        g2d.drawLine(30, 40, 30, height - 35) // Y-axis

        // Draw gridlines
        drawGridlines(100) // Use a default scale

        // Draw legend for all possible log levels
        drawLegend(allLogLevels.toList().sorted())

        // Draw "No data" message
        g2d.color = UiColors.defaultText
        g2d.font = Font("Monospaced", Font.PLAIN, 12)
        g2d.drawString("No log data available", width / 2 - 80, height / 2)
    }

    /**
     * Draw horizontal gridlines
     */
    private fun drawGridlines(maxCount: Int) {
        // Draw horizontal gridlines
        g2d.color = Color.GRAY // Use gray color for gridlines
        g2d.stroke = BasicStroke(0.5f, BasicStroke.CAP_BUTT, BasicStroke.JOIN_MITER, 10.0f, floatArrayOf(2.0f), 0.0f) // Dashed line

        // Draw 5 gridlines
        for (i in 1..5) {
            val y = height - 35 - ((i.toFloat() / 5) * (height - 80))
            g2d.drawLine(30, y.toInt(), width - 30, y.toInt())

            // Draw scale label
            g2d.font = Font("Monospaced", Font.PLAIN, 8)
            g2d.color = Color.GRAY // Use gray color for y-axis numbers
            g2d.drawString((maxCount * i / 5).toString(), 5, y.toInt() + 4)
        }

        // Reset stroke
        g2d.stroke = BasicStroke(1.0f)
    }

    /**
     * Draw the legend for log levels
     */
    private fun drawLegend(levels: List<String>) {
        // Position the legend at the bottom of the chart, below the time labels
        // Increase the space from the bottom to ensure labels are fully visible
        val legendY = height - 20
        g2d.font = Font("Monospaced", Font.PLAIN, 10)

        // Clear the level rectangles map
        levelRectangles.clear()

        // Calculate total width needed for all labels
        val labelWidth = 70 // Width for each label including spacing
        val totalWidth = labelWidth * levels.size
        val startX = (width - totalWidth) / 2 // Center the labels horizontally

        // Draw each level in the legend horizontally
        levels.forEachIndexed { index, level ->
            // Calculate position for this label
            val boxX = startX + (index * labelWidth)
            val boxY = legendY

            // Set color based on log level
            val color = getLevelColor(level)

            // Check if level is selected
            val isSelected = level in selectedLevels

            // Draw selection indicator (background rectangle) if selected
            if (isSelected) {
                g2d.color = Color(color.red, color.green, color.blue, 40) // Semi-transparent background
                g2d.fillRect(boxX - 2, boxY - 2, labelWidth - 4, 14)

                // Draw border around selected level
                g2d.color = color
                g2d.drawRect(boxX - 2, boxY - 2, labelWidth - 4, 14)
            }

            // Draw the color box
            g2d.color = color
            g2d.fillRect(boxX, boxY, 10, 10)

            // Draw the level name
            g2d.color = if (isSelected) UiColors.defaultText else Color.GRAY
            val textX = boxX + 15
            val textY = boxY + 10
            g2d.drawString(level, textX, textY)

            // Store the rectangle for click detection
            val textWidth = g2d.fontMetrics.stringWidth(level)
            levelRectangles[level] = Rectangle(boxX - 2, boxY - 2, labelWidth - 4, 14)
        }
    }

    /**
     * Get the color for a log level
     */
    private fun getLevelColor(level: String): Color {
        return when (level) {
            "INFO" -> UiColors.green
            "WARN" -> UiColors.orange
            "DEBUG" -> UiColors.defaultText
            "ERROR" -> UiColors.red
            "UNKNOWN" -> Color.GRAY
            else -> UiColors.defaultText
        }
    }

    // KeyListener implementation
    override fun keyTyped(e: KeyEvent) {
        // Not used
    }

    override fun keyPressed(e: KeyEvent) {
        // Not used
    }

    override fun keyReleased(e: KeyEvent) {
        // Not used
    }

    // MouseListener implementation
    override fun mouseClicked(e: MouseEvent) {
        // Check if a level label was clicked
        for ((level, rect) in levelRectangles) {
            if (rect.contains(e.point)) {
                // Toggle the level selection
                if (level in selectedLevels) {
                    // Don't allow deselecting the last level
                    if (selectedLevels.size > 1) {
                        selectedLevels.remove(level)
                    }
                } else {
                    selectedLevels.add(level)
                }

                // Notify listener that levels have changed
                onLevelsChanged?.invoke()

                // Repaint the chart
                panel.repaint()
                break
            }
        }
    }

    override fun mousePressed(e: MouseEvent) {
        // Not used
    }

    override fun mouseReleased(e: MouseEvent) {
        // Not used
    }

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

    // MouseWheelListener implementation
    override fun mouseWheelMoved(e: MouseWheelEvent) {
        // Not used
    }

    // MouseMotionListener implementation
    override fun mouseDragged(e: MouseEvent) {
        // Not used
    }

    override fun mouseMoved(e: MouseEvent) {
        // Not used
    }
}
