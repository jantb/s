package widgets

import ComponentOwn
import LogLevel
import State
import app.Domain
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
import java.util.concurrent.locks.ReentrantLock

/**
 * A component that displays a bar chart of log levels over time.
 * Log entries are counted by level (INFO, WARN, DEBUG, ERROR) and shown as stacked bars.
 */
class LogLevelChart(
    x: Int, y: Int, width: Int, height: Int, private val onLevelsChanged: (() -> Unit)? = null
) : ComponentOwn() {

    // Data structure for log counts at a specific time
    private data class TimePoint(val time: Instant, val counts: MutableMap<LogLevel, Int> = mutableMapOf())

    // Chart state
    private val timePoints = mutableListOf<TimePoint>()
    private var startTime: Instant = Instant.now()
    private var endTime: Instant = Instant.now()
    private var currentScaleMax = 100 // Stable scale for bar heights and gridlines
    private var scaleLastChangedTime = Instant.now()
    private var pendingScaleMax = 0 // For delayed scale increases

    // Log levels
    private val allLogLevels = LogLevel.entries.toTypedArray().toList()
    private val levelRectangles = mutableMapOf<LogLevel, Rectangle>()

    // UI properties
    private var image: BufferedImage? = null
    private var g2d: Graphics2D? = null
    private val lock = ReentrantLock()
    private val widthDivision = 6
    private val timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.systemDefault())

    init {
        this.x = x
        this.y = y
        this.width = width
        this.height = height
    }

    /** Updates the chart with new log data. */
    fun updateChart(logs: List<Domain>) {
        if (logs.isEmpty()) return

        lock.lock()
        try {
            // Set time range (assuming logs are ordered oldest to newest)
            startTime = logs.lastOrNull()?.timestamp?.let { Instant.ofEpochMilli(it) } ?: Instant.now()
            endTime = logs.firstOrNull()?.timestamp?.let { Instant.ofEpochMilli(it) } ?: Instant.now()
            if (startTime == endTime) endTime = startTime.plus(1, ChronoUnit.MINUTES)

            val timeInterval = Duration.between(startTime, endTime).dividedBy(width.toLong() / widthDivision)
            val intervalNanos = timeInterval.toNanos()

            // Recreate time points
            timePoints.clear()
            (0 until width.toLong() / widthDivision).forEach { i ->
                timePoints.add(TimePoint(startTime.plus(timeInterval.multipliedBy(i))))
            }

            // Assign logs to time points efficiently
            logs.forEach { domain ->
                val level = domain.level
                val durationSinceStart = Duration.between(startTime, Instant.ofEpochMilli(domain.timestamp)).toNanos()
                val index = if (intervalNanos > 0) {
                    (durationSinceStart / intervalNanos).toInt().coerceIn(0, timePoints.size - 1)
                } else 0
                timePoints[index].counts[level] = timePoints[index].counts.getOrDefault(level, 0) + 1
            }

            // Update scale
            val newMaxTotal = timePoints.maxOf { it.counts.values.sum() }
            updateScale(newMaxTotal)
        } finally {
            lock.unlock()
        }
    }

    /** Updates the chart scale with hysteresis. */
    private fun updateScale(newMaxTotal: Int) {
        val now = Instant.now()
        val timeSinceLastChange = Duration.between(scaleLastChangedTime, now).seconds

        when {
            newMaxTotal > currentScaleMax * 1.5 -> {
                pendingScaleMax = newMaxTotal
                if (timeSinceLastChange > 2 || newMaxTotal > currentScaleMax * 4) {
                    currentScaleMax = (newMaxTotal * 1.2).toInt()
                    scaleLastChangedTime = now
                    pendingScaleMax = 0
                }
            }

            newMaxTotal < currentScaleMax / 3 && timeSinceLastChange > 1 -> {
                currentScaleMax = maxOf(newMaxTotal * 2, 5)
                scaleLastChangedTime = now
            }
        }
    }


    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        lock.lock()
        try {
            // Reinitialize image if dimensions change or not yet initialized
            if (image == null || this.width != width || this.height != height) {
                g2d?.dispose()
                image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
                g2d = image!!.createGraphics()
                this.width = width
                this.height = height
                this.x = x
                this.y = y
            }

            // Clear and draw
            g2d!!.apply {
                color = UiColors.background
                fillRect(0, 0, width, height)
                drawChart()
            }

            return image!!
        } finally {
            lock.unlock()
        }
    }

    private fun Graphics2D.drawChart() {
        if (timePoints.isEmpty()) {
            drawEmptyChart()
            return
        }

        val levels = allLogLevels.toList()
        val timeSlotWidth = (width - 60).toFloat() / timePoints.size.coerceAtLeast(1)

        // Draw gridlines and axis
        drawGridlines(currentScaleMax)
        color = Color.GRAY
        drawLine(30, height - 35, width - 30, height - 35)

        // Draw bars and time labels
        timePoints.forEachIndexed { index, timePoint ->
            val xPosBase = 30 + (index * timeSlotWidth).toInt()
            if (index % 25 == 0 || index == timePoints.size - 1) {
                font = Font("Monospaced", Font.PLAIN, 12)
                color = Color.GRAY
                drawString(timeFormatter.format(timePoint.time), xPosBase, height - 15)
            }

            var currentHeight = 0
            levels.forEach { level ->
                if (level in State.levels.get()) {
                    val count = timePoint.counts.getOrDefault(level, 0)
                    if (count > 0) {
                        val barHeight = ((count.toFloat() / currentScaleMax) * (height - 80)).toInt().coerceAtLeast(1)
                        color = getLevelColor(level)
                        fillRect(
                            xPosBase, height - 35 - currentHeight - barHeight, timeSlotWidth.toInt() - 1, barHeight
                        )
                        currentHeight += barHeight
                    }
                }
            }
        }

        drawLegend(levels)
    }

    private fun Graphics2D.drawEmptyChart() {
        color = UiColors.background
        fillRect(0, 0, width, height)
        color = Color.GRAY
        drawLine(30, height - 35, width - 30, height - 35) // X-axis
        drawLine(30, 40, 30, height - 35) // Y-axis
        drawGridlines(100)
        drawLegend(allLogLevels.toList())
    }

    private fun Graphics2D.drawGridlines(maxCount: Int) {
        color = Color.DARK_GRAY
        font = Font("Monospaced", Font.PLAIN, 10)

        (1..5).forEach { i ->
            val y = height - 35 - ((i.toFloat() / 5) * (height - 80)).toInt()
            drawLine(30, y, width - 30, y)
            drawString((maxCount * i / 5).toString(), 5, y + 4)
        }
        stroke = BasicStroke(1.0f)
    }

    private fun Graphics2D.drawLegend(levels: List<LogLevel>) {
        font = Font("Monospaced", Font.PLAIN, 10)
        levelRectangles.clear()
        val labelWidth = 90
        val totalWidth = labelWidth * levels.size
        val startX = (width - totalWidth) / 2

        levels.forEachIndexed { index, level ->
            val boxX = startX + (index * labelWidth)
            val boxY = 15
            val isSelected = level in State.levels.get()

            if (isSelected) {
                color = getLevelColor(level).let { Color(it.red, it.green, it.blue, 40) }
                fillRect(boxX - 2, boxY - 2, labelWidth - 4, 14)
                color = getLevelColor(level)
                drawRect(boxX - 2, boxY - 2, labelWidth - 4, 14)
            }

            color = getLevelColor(level)
            fillRect(boxX, boxY, 10, 10)
            color = if (isSelected) UiColors.defaultText else Color.GRAY
            drawString(level.name, boxX + 15, boxY + 10)

            levelRectangles[level] = Rectangle(boxX - 2, boxY - 2, labelWidth - 4, 14)
        }
    }

    private fun getLevelColor(level: LogLevel): Color = when (level) {

        LogLevel.INFO -> UiColors.green
        LogLevel.WARN -> UiColors.orange
        LogLevel.DEBUG -> UiColors.defaultText
        LogLevel.ERROR -> UiColors.red
        LogLevel.UNKNOWN -> Color.GRAY
    }

    override fun mouseClicked(e: MouseEvent) {
        levelRectangles.entries.find { it.value.contains(e.point) }?.key?.let { level ->
            if (level in State.levels.get() && State.levels.get().size > 1) {
                val logLevels = State.levels.get()
                logLevels.remove(level)
                State.levels.set(
                    logLevels
                )
            } else State.levels.get().add(level)
            onLevelsChanged?.invoke()
        }
    }

    // Unused overrides omitted for brevity
    override fun repaint(componentOwn: ComponentOwn) {}
    override fun keyTyped(e: KeyEvent) {}
    override fun keyPressed(e: KeyEvent) {}
    override fun keyReleased(e: KeyEvent) {}
    override fun mousePressed(e: MouseEvent) {}
    override fun mouseReleased(e: MouseEvent) {}
    override fun mouseEntered(e: MouseEvent) {
        mouseInside = true
    }

    override fun mouseExited(e: MouseEvent) {
        mouseInside = false
    }

    override fun mouseWheelMoved(e: MouseWheelEvent) {}
    override fun mouseDragged(e: MouseEvent) {}
    override fun mouseMoved(e: MouseEvent) {}
}