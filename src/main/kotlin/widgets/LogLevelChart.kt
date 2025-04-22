package widgets

import ComponentOwn
import SlidePanel
import app.Domain
import util.UiColors
import java.awt.Font
import java.awt.Graphics2D
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
    height: Int
) : ComponentOwn() {

    // Data structure to store log counts by time period and level
    private data class TimePoint(val time: Instant, val counts: MutableMap<String, Int> = mutableMapOf())

    // List of time points for the chart
    private val timePoints = mutableListOf<TimePoint>()

    // Time range for the chart
    private var startTime: Instant = Instant.now()
    private var endTime: Instant = Instant.now()

    // Number of time divisions to display
    private val numTimeDivisions = 10

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
     */
    fun updateChart(logs: List<Domain>) {
        if (logs.isEmpty()) {
            timePoints.clear()
            panel.repaint()
            return
        }

        // Determine time range from logs
        startTime = logs.minByOrNull { it.timestamp }?.timestamp ?: Instant.now()
        endTime = logs.maxByOrNull { it.timestamp }?.timestamp ?: Instant.now()

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
        logs.forEach { domain ->
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
        if (timePoints.isEmpty()) {
            // No data to display
            g2d.color = UiColors.defaultText
            g2d.font = Font("Monospaced", Font.PLAIN, 12)
            g2d.drawString("No log data available", 10, height / 2)
            return
        }

        // Get all unique log levels across all time points
        val allLevels = ArrayList<String>()
        // Create a defensive copy of timePoints to prevent concurrent modification
        val timePointsCopy = ArrayList(timePoints)
        timePointsCopy.forEach { timePoint ->
            if (timePoint.counts.isNotEmpty()) {
                // Create a defensive copy of the keys to prevent concurrent modification
                allLevels.addAll(ArrayList(timePoint.counts.keys))
            }
        }
        // Create a defensive copy of the unique levels
        val uniqueLevels = ArrayList(allLevels.distinct().sorted())

        // Calculate the maximum count for scaling
        var maxCount = 0
        // Use the same defensive copy of timePoints
        timePointsCopy.forEach { timePoint ->
            // Create a defensive copy of the values to prevent concurrent modification
            val valuesCopy = ArrayList(timePoint.counts.values)
            valuesCopy.maxOrNull()?.let { max ->
                if (max > maxCount) maxCount = max
            }
        }
        if (maxCount == 0) return

        // Calculate bar width based on number of time points
        val timeSlotWidth = (width - 60) / timePointsCopy.size.coerceAtLeast(1)
        val barWidth = (timeSlotWidth / (uniqueLevels.size + 1)).coerceAtLeast(3) // +1 for spacing, minimum width of 3
        val barSpacing = 2

        // Draw time axis
        g2d.color = UiColors.defaultText
        g2d.drawLine(30, height - 35, width - 30, height - 35)

        // Draw bars for each time point
        timePointsCopy.forEachIndexed { timeIndex, timePoint ->
            val xPosBase = 30 + (timeIndex * timeSlotWidth)

            // Draw time label (only for some time points to avoid crowding)
            if (timeIndex % 2 == 0 || timeIndex == timePointsCopy.size - 1) {
                val formatter = DateTimeFormatter.ofPattern("HH:mm:ss")
                    .withZone(ZoneId.systemDefault())
                val timeLabel = formatter.format(timePoint.time)
                g2d.font = Font("Monospaced", Font.PLAIN, 8)
                g2d.drawString(timeLabel, xPosBase, height - 15)
            }

            // Draw bars for each log level at this time point
            // Create a defensive copy of the counts to prevent concurrent modification
            val countsCopy = HashMap(timePoint.counts)
            uniqueLevels.forEachIndexed { levelIndex, level ->
                val count = countsCopy.getOrDefault(level, 0)
                if (count > 0) {
                    // Calculate bar height (scaled to fit in the chart)
                    val barHeight = ((count.toFloat() / maxCount) * (height - 80)).toInt()

                    // Set bar color based on log level
                    g2d.color = when (level) {
                        "INFO" -> UiColors.green
                        "WARN" -> UiColors.orange
                        "DEBUG" -> UiColors.defaultText
                        "ERROR" -> UiColors.red
                        else -> UiColors.defaultText
                    }

                    // Calculate x position for this bar
                    val xPos = xPosBase + (levelIndex * barWidth)

                    // Draw the bar
                    g2d.fillRect(xPos, height - 40 - barHeight, barWidth - barSpacing, barHeight)

                    // Draw count label if bar is tall enough
                    if (barHeight > 15) {
                        g2d.color = UiColors.defaultText
                        g2d.font = Font("Monospaced", Font.PLAIN, 8)
                        // Position the label above the bar with some padding
                        g2d.drawString(count.toString(), xPos, height - 45 - barHeight)
                    }
                }
            }
        }

        // Draw legend for log levels
        val legendX = 30
        val legendY = 20
        g2d.font = Font("Monospaced", Font.PLAIN, 10)

        // Use the same defensive copy of uniqueLevels
        ArrayList(uniqueLevels).forEachIndexed { index, level ->
            g2d.color = when (level) {
                "INFO" -> UiColors.green
                "WARN" -> UiColors.orange
                "DEBUG" -> UiColors.defaultText
                "ERROR" -> UiColors.red
                else -> UiColors.defaultText
            }

            g2d.fillRect(legendX, legendY + (index * 15), 10, 10)
            g2d.color = UiColors.defaultText
            g2d.drawString(level, legendX + 15, legendY + 10 + (index * 15))
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
        // Not used
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
