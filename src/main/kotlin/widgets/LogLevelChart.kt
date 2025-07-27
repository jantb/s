package widgets

import ComponentOwn
import LogLevel
import State
import app.DomainLine
import kotlinx.datetime.*
import util.UiColors
import java.awt.*
import java.awt.event.KeyEvent
import java.awt.event.MouseEvent
import java.awt.event.MouseWheelEvent
import java.awt.geom.RoundRectangle2D
import java.awt.image.BufferedImage
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.concurrent.locks.ReentrantLock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.DurationUnit

/**
 * A component that displays a bar chart of log levels over time.
 * Log entries are counted by level (INFO, WARN, DEBUG, ERROR) and shown as stacked bars.
 */
class LogLevelChart(
    x: Int, y: Int, width: Int, height: Int, private val onLevelsChanged: (() -> Unit)? = null
) : ComponentOwn() {

    // Data structure for log counts at a specific time
    private data class TimePoint(
        val time: kotlinx.datetime.Instant,
        val counts: IntArray = IntArray(LogLevel.entries.size)
    ) {
        // Get total count for all levels
        fun getTotal(): Int = counts.sum()

        // Get count for a specific level
        fun getCount(level: LogLevel): Int = counts[level.ordinal]

        // Increment count for a specific level
        fun incrementCount(level: LogLevel) {
            counts[level.ordinal]++
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as TimePoint

            if (time != other.time) return false
            if (!counts.contentEquals(other.counts)) return false

            return true
        }

        override fun hashCode(): Int {
            var result = time.hashCode()
            result = 31 * result + counts.contentHashCode()
            return result
        }
    }

    // Chart state
    private val timePoints = mutableListOf<TimePoint>()
    private var startTime: kotlinx.datetime.Instant = Clock.System.now()
    private var endTime: kotlinx.datetime.Instant = Clock.System.now()
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
    
    // Enhanced UI properties
    private var hoveredBar: Pair<Int, LogLevel>? = null
    private var tooltipInfo: TooltipInfo? = null
    
    // Zoom and pan properties
    private var zoomLevel = 1.0
    private var panOffset = 0.0
    private var isDragging = false
    private var lastMouseX = 0
    private var lastMouseY = 0
    
    private data class TooltipInfo(
        val x: Int,
        val y: Int,
        val timePoint: TimePoint,
        val level: LogLevel,
        val count: Int
    )

    init {
        this.x = x
        this.y = y
        this.width = width
        this.height = height
    }

    /** Updates the chart with new log data. */
    fun updateChart(logs: List<DomainLine>) {
        if (logs.isEmpty()) return

        lock.lock()
        try {
            // Set time range (assuming logs are ordered newest to oldest)
            startTime = logs.lastOrNull()?.timestamp?.let { kotlinx.datetime.Instant.fromEpochMilliseconds(it) } ?: Clock.System.now()
            endTime = logs.firstOrNull()?.timestamp?.let { kotlinx.datetime.Instant.fromEpochMilliseconds(it) } ?: Clock.System.now()
            if (startTime == endTime) endTime = startTime.plus(1.minutes)

            val durationTotal = startTime.until(endTime, DateTimeUnit.MILLISECOND)
            val interval = durationTotal / (width.toLong() / widthDivision)

            // Recreate time points
            timePoints.clear()
            (0 until width.toLong() / widthDivision).forEach { i ->
                timePoints.add(TimePoint(startTime.plus(interval * i, DateTimeUnit.MILLISECOND)))
            }

            // Assign logs to time points efficiently
            logs.forEach { domain ->
                val level = domain.level
                val durationSinceStart = startTime.until(kotlinx.datetime.Instant.fromEpochMilliseconds(domain.timestamp), DateTimeUnit.MILLISECOND)
                val index = if (interval > 0) {
                    (durationSinceStart / interval).toInt().coerceIn(0, timePoints.size - 1)
                } else 0
                timePoints[index].incrementCount(level)
            }

            // Update scale
            val newMaxTotal = timePoints.maxOfOrNull { it.getTotal() } ?: 0
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

            // Clear and draw with anti-aliasing
            g2d!!.apply {
                setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
                setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON)
                setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY)
                
                color = UiColors.background
                fillRect(0, 0, width, height)
                drawChart()
                drawTooltip()
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
        
        // Apply zoom and pan transformations
        val chartWidth = (width - 60).toFloat()
        val zoomedWidth = chartWidth * zoomLevel.toFloat()
        val panPixels = (panOffset * chartWidth).toFloat()
        val timeSlotWidth = zoomedWidth / timePoints.size.coerceAtLeast(1)

        // Draw enhanced gridlines and axis (adjusted for header height)
        drawEnhancedGridlines(currentScaleMax)
        
        // Draw main axis with better styling
        stroke = BasicStroke(2f)
        color = UiColors.defaultText.darker()
        drawLine(30, height - 35, width - 30, height - 35)
        stroke = BasicStroke(1f)

        // Create a date formatter
        val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault())

        // Track the last date to detect date changes
        var lastDate: String? = null

        // Draw bars and time labels with zoom and pan
        timePoints.forEachIndexed { index, timePoint ->
            val xPosBase = 30 + (index * timeSlotWidth + panPixels).toInt()
            
            // Skip rendering if bar is outside visible area (performance optimization)
            if (xPosBase + timeSlotWidth < 30 || xPosBase > width - 30) {
                return@forEachIndexed
            }
            
            val javaInstant = timePoint.time.toJavaInstant()

            // Get current date string
            val currentDate = dateFormatter.format(javaInstant)

            // Check if date changed
            if (lastDate != currentDate) {
                // Draw date change indicator - a prominent vertical line
                val dateChangeStroke = BasicStroke(2f)
                val originalStroke = stroke
                stroke = dateChangeStroke
                color = UiColors.teal
                drawLine(xPosBase, 55, xPosBase, height - 35)
                stroke = originalStroke

                // Add date label
                font = Font("Monospaced", Font.BOLD, 12)
                drawString(currentDate, xPosBase + 5, 70)

                // Reset to regular color for other elements
                color = Color.GRAY
            }

            // Draw regular time labels at intervals
            if (index % 25 == 0 || index == timePoints.size - 1) {
                font = Font("Monospaced", Font.PLAIN, 12)
                color = Color.GRAY
                drawString(timeFormatter.format(javaInstant), xPosBase, height - 15)
            }

            // Update last date
            lastDate = currentDate

            var currentHeight = 0
            levels.forEach { level ->
                if (level in State.levels.get()) {
                    val count = timePoint.getCount(level)
                    if (count > 0) {
                        val maxChartHeight = height - 55 - 35  // From header bottom (55) to axis (35 from bottom)
                        val barHeight = ((count.toFloat() / currentScaleMax) * maxChartHeight).toInt().coerceAtLeast(1)
                        val barWidth = timeSlotWidth.toInt() - 2
                        val barX = xPosBase
                        val barY = height - 35 - currentHeight - barHeight
                        
                        // Ensure bar doesn't extend above header
                        val adjustedBarY = barY.coerceAtLeast(55)
                        val adjustedBarHeight = if (adjustedBarY > barY) barHeight - (adjustedBarY - barY) else barHeight
                        
                        // Draw bar with gradient and rounded corners (only if height > 0)
                        if (adjustedBarHeight > 0) {
                            drawEnhancedBar(barX, adjustedBarY, barWidth, adjustedBarHeight, level, count, timePoint)
                        }
                        currentHeight += barHeight
                    }
                }
            }
        }

        val totalMessages = timePoints.sumOf { it.getTotal() }
        val durationSecs = startTime.until(endTime, DateTimeUnit.SECOND).seconds.toInt(DurationUnit.SECONDS).coerceAtLeast(1)
        val avgMps = totalMessages.toFloat() / durationSecs

        // Enhanced header information
        drawEnhancedHeader(totalMessages, avgMps, durationSecs)
        drawZoomIndicator()
        drawLegend(levels)
    }

    private fun Graphics2D.drawEmptyChart() {
        color = UiColors.background
        fillRect(0, 0, width, height)
        
        // Draw enhanced empty chart
        drawEnhancedGridlines(100)
        
        // Draw main axes with better styling
        stroke = BasicStroke(2f)
        color = UiColors.defaultText.darker()
        drawLine(30, height - 35, width - 30, height - 35) // X-axis
        drawLine(30, 55, 30, height - 35) // Y-axis
        stroke = BasicStroke(1f)
        
        // Add "No Data" message
        font = Font("JetBrains Mono", Font.ITALIC, 14)
        color = UiColors.defaultText.darker()
        val message = "No log data available"
        val metrics = fontMetrics
        val messageWidth = metrics.stringWidth(message)
        drawString(message, (width - messageWidth) / 2, height / 2)
        
        drawLegend(allLogLevels.toList())
    }
    
    private fun Graphics2D.drawEnhancedHeader(totalMessages: Int, avgMps: Float, durationSecs: Int) {
        // Create header background
        val headerHeight = 50  // Increased height to accommodate two lines
        val gradient = GradientPaint(
            0f, 0f, Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200),
            0f, headerHeight.toFloat(), Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 100)
        )
        paint = gradient
        fillRect(0, 0, width, headerHeight)
        
        // Header border
        color = UiColors.defaultText.darker()
        stroke = BasicStroke(1f)
        drawLine(0, headerHeight, width, headerHeight)
        
        // Time range information
        val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.systemDefault())
        val startDateStr = dateFormatter.format(startTime.toJavaInstant())
        val endDateStr = dateFormatter.format(endTime.toJavaInstant())
        
        // First line - Time range
        font = Font("JetBrains Mono", Font.BOLD, 11)
        color = UiColors.defaultText.brighter()
        val timeRangeText = if (startDateStr == endDateStr) {
            "$startDateStr ${timeFormatter.format(startTime.toJavaInstant())} - ${timeFormatter.format(endTime.toJavaInstant())}"
        } else {
            "$startDateStr ${timeFormatter.format(startTime.toJavaInstant())} - $endDateStr ${timeFormatter.format(endTime.toJavaInstant())}"
        }
        drawString(timeRangeText, 10, 18)
        
        // Second line - Duration and Statistics
        font = Font("JetBrains Mono", Font.PLAIN, 11)
        color = UiColors.defaultText
        val duration = endTime.minus(startTime)
        val durationText = "Duration: $duration"
        drawString(durationText, 10, 35)
        
        // Right side - Statistics on second line
        font = Font("JetBrains Mono", Font.BOLD, 11)
        color = UiColors.teal
        val statsText = "Total: $totalMessages | Avg: %.1f/s".format(avgMps)
        val statsWidth = fontMetrics.stringWidth(statsText)
        drawString(statsText, width - statsWidth - 10, 35)
    }
    
    private fun Graphics2D.drawZoomIndicator() {
        if (zoomLevel != 1.0 || panOffset != 0.0) {
            // Draw zoom indicator in bottom right corner
            val indicatorWidth = 120
            val indicatorHeight = 25
            val indicatorX = width - indicatorWidth - 10
            val indicatorY = height - indicatorHeight - 10
            
            // Background
            color = Color(0, 0, 0, 150)
            fillRoundRect(indicatorX, indicatorY, indicatorWidth, indicatorHeight, 5, 5)
            
            // Border
            color = UiColors.defaultText.darker()
            drawRoundRect(indicatorX, indicatorY, indicatorWidth, indicatorHeight, 5, 5)
            
            // Text
            font = Font("JetBrains Mono", Font.PLAIN, 10)
            color = UiColors.defaultText
            drawString("Zoom: %.1fx".format(zoomLevel), indicatorX + 5, indicatorY + 12)
            if (panOffset != 0.0) {
                drawString("Pan: %.1f".format(panOffset), indicatorX + 5, indicatorY + 22)
            }
        }
    }

    private fun Graphics2D.drawEnhancedGridlines(maxCount: Int) {
        // Draw subtle grid lines
        stroke = BasicStroke(1f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 0f, floatArrayOf(2f, 4f), 0f)
        color = Color(UiColors.defaultText.red, UiColors.defaultText.green, UiColors.defaultText.blue, 30)
        
        (1..5).forEach { i ->
            val y = height - 35 - ((i.toFloat() / 5) * (height - 105)).toInt()
            drawLine(30, y, width - 30, y)
        }
        
        // Draw Y-axis labels with better styling
        stroke = BasicStroke(1f)
        color = UiColors.defaultText.brighter()
        font = Font("JetBrains Mono", Font.PLAIN, 11)
        
        (1..5).forEach { i ->
            val y = height - 35 - ((i.toFloat() / 5) * (height - 105)).toInt()
            val value = (maxCount * i / 5).toString()
            drawString(value, 5, y + 4)
        }
    }
    
    private fun Graphics2D.drawEnhancedBar(x: Int, y: Int, width: Int, height: Int, level: LogLevel, count: Int, timePoint: TimePoint) {
        val baseColor = getLevelColor(level)
        
        // Create gradient paint
        val gradient = GradientPaint(
            x.toFloat(), y.toFloat(), baseColor.brighter(),
            x.toFloat(), (y + height).toFloat(), baseColor.darker()
        )
        
        // Check if this bar is hovered
        val isHovered = hoveredBar?.first == timePoints.indexOf(timePoint) && hoveredBar?.second == level
        
        if (isHovered) {
            // Draw glow effect for hovered bar
            val glowColor = Color(baseColor.red, baseColor.green, baseColor.blue, 100)
            paint = GradientPaint(
                (x - 2).toFloat(), (y - 2).toFloat(), glowColor,
                (x - 2).toFloat(), (y + height + 4).toFloat(), Color(0, 0, 0, 0)
            )
            fill(RoundRectangle2D.Float((x - 2).toFloat(), (y - 2).toFloat(), (width + 4).toFloat(), (height + 4).toFloat(), 4f, 4f))
        }
        
        // Draw main bar with rounded corners
        paint = gradient
        fill(RoundRectangle2D.Float(x.toFloat(), y.toFloat(), width.toFloat(), height.toFloat(), 3f, 3f))
        
        // Add subtle border
        color = baseColor.darker().darker()
        stroke = BasicStroke(0.5f)
        draw(RoundRectangle2D.Float(x.toFloat(), y.toFloat(), width.toFloat(), height.toFloat(), 3f, 3f))
    }
    
    private fun Graphics2D.drawTooltip() {
        tooltipInfo?.let { tooltip ->
            val tooltipWidth = 200
            val tooltipHeight = 95
            val padding = 8
            
            // Position tooltip to avoid edges
            var tooltipX = tooltip.x + 10
            var tooltipY = tooltip.y - tooltipHeight - 10
            
            if (tooltipX + tooltipWidth > width) tooltipX = tooltip.x - tooltipWidth - 10
            if (tooltipY < 0) tooltipY = tooltip.y + 20
            
            // Draw tooltip background with shadow
            color = Color(0, 0, 0, 100)
            fillRoundRect(tooltipX + 2, tooltipY + 2, tooltipWidth, tooltipHeight, 8, 8)
            
            color = Color(40, 42, 46)
            fillRoundRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight, 8, 8)
            
            color = UiColors.defaultText.darker()
            stroke = BasicStroke(1f)
            drawRoundRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight, 8, 8)
            
            // Draw tooltip content
            font = Font("JetBrains Mono", Font.BOLD, 12)
            color = UiColors.defaultText
            
            val timeStr = timeFormatter.format(tooltip.timePoint.time.toJavaInstant())
            drawString("Time: $timeStr", tooltipX + padding, tooltipY + padding + 15)
            
            color = getLevelColor(tooltip.level)
            drawString("${tooltip.level.name}: ${tooltip.count}", tooltipX + padding, tooltipY + padding + 35)
            
            color = UiColors.defaultText.darker()
            font = Font("JetBrains Mono", Font.PLAIN, 11)
            drawString("Total: ${tooltip.timePoint.getTotal()}", tooltipX + padding, tooltipY + padding + 55)
            
            // Add percentage information
            val percentage = if (tooltip.timePoint.getTotal() > 0) {
                (tooltip.count.toFloat() / tooltip.timePoint.getTotal() * 100)
            } else 0f
            drawString("Percentage: %.1f%%".format(percentage), tooltipX + padding, tooltipY + padding + 70)
        }
    }

    private fun Graphics2D.drawLegend(levels: List<LogLevel>) {
        font = Font("JetBrains Mono", Font.PLAIN, 11)
        levelRectangles.clear()
        val labelWidth = 90
        val totalWidth = labelWidth * levels.size
        val startX = (width - totalWidth) / 2

        levels.forEachIndexed { index, level ->
            val boxX = startX + (index * labelWidth)
            val boxY = 15
            val isSelected = level in State.levels.get()

            // Enhanced selection background
            if (isSelected) {
                val selectionColor = getLevelColor(level)
                color = Color(selectionColor.red, selectionColor.green, selectionColor.blue, 60)
                fillRoundRect(boxX - 4, boxY - 4, labelWidth - 2, 18, 6, 6)
                
                color = Color(selectionColor.red, selectionColor.green, selectionColor.blue, 120)
                stroke = BasicStroke(1.5f)
                drawRoundRect(boxX - 4, boxY - 4, labelWidth - 2, 18, 6, 6)
                stroke = BasicStroke(1f)
            }

            // Enhanced color indicator with gradient
            val levelColor = getLevelColor(level)
            val gradient = GradientPaint(
                boxX.toFloat(), boxY.toFloat(), levelColor.brighter(),
                boxX.toFloat(), (boxY + 12).toFloat(), levelColor.darker()
            )
            paint = gradient
            fillRoundRect(boxX, boxY, 12, 12, 3, 3)
            
            // Add subtle border to color indicator
            color = levelColor.darker()
            stroke = BasicStroke(0.5f)
            drawRoundRect(boxX, boxY, 12, 12, 3, 3)
            stroke = BasicStroke(1f)
            
            // Enhanced text rendering
            color = if (isSelected) UiColors.defaultText.brighter() else UiColors.defaultText.darker()
            font = Font("JetBrains Mono", if (isSelected) Font.BOLD else Font.PLAIN, 11)
            drawString(level.name, boxX + 18, boxY + 10)

            levelRectangles[level] = Rectangle(boxX - 4, boxY - 4, labelWidth - 2, 18)
        }
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

    override fun repaint(componentOwn: ComponentOwn) {}
    override fun keyTyped(e: KeyEvent) {}
    
    override fun keyPressed(e: KeyEvent) {
        when (e.keyCode) {
            KeyEvent.VK_PLUS, KeyEvent.VK_EQUALS -> {
                // Zoom in
                zoomLevel = (zoomLevel * 1.2).coerceAtMost(10.0)
            }
            KeyEvent.VK_MINUS -> {
                // Zoom out
                zoomLevel = (zoomLevel / 1.2).coerceAtLeast(0.1)
            }
            KeyEvent.VK_0 -> {
                // Reset zoom and pan
                zoomLevel = 1.0
                panOffset = 0.0
            }
            KeyEvent.VK_LEFT -> {
                // Pan left
                if (zoomLevel > 1.0) {
                    val minPan = -(zoomLevel - 1).coerceAtLeast(0.0)
                    panOffset = (panOffset - 0.1).coerceAtLeast(minPan)
                }
            }
            KeyEvent.VK_RIGHT -> {
                // Pan right
                if (zoomLevel > 1.0) {
                    panOffset = (panOffset + 0.1).coerceAtMost(0.0)
                }
            }
            KeyEvent.VK_R -> {
                // Reset all filters
                State.levels.get().clear()
                State.levels.get().addAll(allLogLevels)
                onLevelsChanged?.invoke()
            }
        }
    }
    
    override fun keyReleased(e: KeyEvent) {}
    override fun mousePressed(e: MouseEvent) {
        isDragging = true
        lastMouseX = e.x
        lastMouseY = e.y
    }
    
    override fun mouseReleased(e: MouseEvent) {
        isDragging = false
    }
    override fun mouseEntered(e: MouseEvent) {
        mouseInside = true
    }

    override fun mouseExited(e: MouseEvent) {
        mouseInside = false
    }

    override fun mouseWheelMoved(e: MouseWheelEvent) {
        // Zoom functionality
        val zoomFactor = if (e.wheelRotation < 0) 1.1 else 0.9
        val newZoomLevel = (zoomLevel * zoomFactor).coerceIn(0.1, 10.0)
        
        if (newZoomLevel != zoomLevel) {
            zoomLevel = newZoomLevel
            // Adjust pan offset to zoom towards mouse position
            val mouseRatio = (e.x - 30).toDouble() / (width - 60).coerceAtLeast(1)
            panOffset = (panOffset + mouseRatio) * zoomFactor - mouseRatio
            
            // Fix the coerceIn range - ensure min <= max
            val minPan = -(zoomLevel - 1).coerceAtLeast(0.0)
            val maxPan = 0.0
            panOffset = panOffset.coerceIn(minPan, maxPan)
        }
    }
    
    override fun mouseDragged(e: MouseEvent) {
        if (isDragging && zoomLevel > 1.0) {
            val deltaX = e.x - lastMouseX
            val panSensitivity = 0.001 * zoomLevel
            panOffset += deltaX * panSensitivity
            
            // Fix the coerceIn range - ensure min <= max
            val minPan = -(zoomLevel - 1).coerceAtLeast(0.0)
            val maxPan = 0.0
            panOffset = panOffset.coerceIn(minPan, maxPan)
        }
        lastMouseX = e.x
        lastMouseY = e.y
    }
    override fun mouseMoved(e: MouseEvent) {
        // Handle hover effects for bars with zoom and pan
        val chartWidth = (width - 60).toFloat()
        val zoomedWidth = chartWidth * zoomLevel.toFloat()
        val panPixels = (panOffset * chartWidth).toFloat()
        val timeSlotWidth = zoomedWidth / timePoints.size.coerceAtLeast(1)
        val mouseX = e.x - 30
        val mouseY = e.y
        
        if (mouseX >= 0 && mouseX < width - 60 && mouseY >= 55 && mouseY <= height - 35) {
            // Adjust mouse position for zoom and pan
            val adjustedMouseX = mouseX - panPixels
            val timeIndex = (adjustedMouseX / timeSlotWidth).toInt().coerceIn(0, timePoints.size - 1)
            val timePoint = timePoints.getOrNull(timeIndex)
            
            if (timePoint != null) {
                // Find which level bar is being hovered
                var currentHeight = 0
                var hoveredLevel: LogLevel? = null
                
                for (level in allLogLevels.reversed()) {
                    if (level in State.levels.get()) {
                        val count = timePoint.getCount(level)
                        if (count > 0) {
                            val maxChartHeight = height - 55 - 35  // From header bottom (55) to axis (35 from bottom)
                            val barHeight = ((count.toFloat() / currentScaleMax) * maxChartHeight).toInt().coerceAtLeast(1)
                            val barY = height - 35 - currentHeight - barHeight
                            val adjustedBarY = barY.coerceAtLeast(55)
                            val adjustedBarHeight = if (adjustedBarY > barY) barHeight - (adjustedBarY - barY) else barHeight
                            
                            val barTop = adjustedBarY
                            val barBottom = adjustedBarY + adjustedBarHeight
                            
                            if (mouseY >= barTop && mouseY <= barBottom) {
                                hoveredLevel = level
                                tooltipInfo = TooltipInfo(e.x, e.y, timePoint, level, count)
                                break
                            }
                            currentHeight += barHeight
                        }
                    }
                }
                
                hoveredBar = if (hoveredLevel != null) timeIndex to hoveredLevel else null
                if (hoveredLevel == null) tooltipInfo = null
            } else {
                hoveredBar = null
                tooltipInfo = null
            }
        } else {
            hoveredBar = null
            tooltipInfo = null
        }
    }
}

fun getLevelColor(level: LogLevel): Color = when (level) {
    LogLevel.INFO -> Color(76, 175, 80)      // Material Green
    LogLevel.WARN -> Color(255, 152, 0)      // Material Orange
    LogLevel.DEBUG -> Color(96, 125, 139)    // Material Blue Grey
    LogLevel.ERROR -> Color(244, 67, 54)     // Material Red
    LogLevel.UNKNOWN -> Color(158, 158, 158) // Material Grey
    LogLevel.KAFKA -> Color(0, 188, 212)     // Material Cyan
}

fun getLevelColorLight(level: LogLevel): Color = when (level) {
    LogLevel.INFO -> Color(129, 199, 132)    // Light Material Green
    LogLevel.WARN -> Color(255, 183, 77)     // Light Material Orange
    LogLevel.DEBUG -> Color(144, 164, 174)   // Light Material Blue Grey
    LogLevel.ERROR -> Color(239, 154, 154)   // Light Material Red
    LogLevel.UNKNOWN -> Color(189, 189, 189) // Light Material Grey
    LogLevel.KAFKA -> Color(77, 208, 225)    // Light Material Cyan
}