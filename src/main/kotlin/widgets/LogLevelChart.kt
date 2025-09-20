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
    x: Int, y: Int, width: Int, height: Int,
    private val onLevelsChanged: (() -> Unit)? = null,
    private val onBarClicked: ((indexOffset: Int) -> Unit)? = null
) : ComponentOwn() {

    // Data structure for log counts at a specific time
    data class TimePoint(
        val time: kotlinx.datetime.Instant,
        val counts: IntArray = IntArray(LogLevel.entries.size),
        val logEntries: MutableList<DomainLine> = mutableListOf() // Store actual log entries
    ) {
        // Get total count for all levels
        fun getTotal(): Int = counts.sum()

        // Get count for a specific level
        fun getCount(level: LogLevel): Int = counts[level.ordinal]

        // Increment count for a specific level and store the log entry
        fun incrementCount(level: LogLevel, logEntry: DomainLine) {
            counts[level.ordinal]++
            logEntries.add(logEntry)
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
    private val timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.of("UTC"))
    private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"))

    // Dynamic margin calculation
    private var leftMargin = 30 // Default left margin, will be adjusted based on label width
    
    // Enhanced UI properties
    private var hoveredBar: Pair<Int, LogLevel>? = null
    private var tooltipInfo: TooltipInfo? = null
    
    // Mouse interaction properties
    private var lastMouseX = 0
    private var lastMouseY = 0
    
    // Performance optimization caches
    private val gradientCache = mutableMapOf<LogLevel, GradientPaint>()
    private val colorCache = mutableMapOf<LogLevel, Color>()
    private var cachedVisibleRange: Pair<Int, Int>? = null
    private var lastRenderWidth = 0
    private var lastRenderHeight = 0
    private var cachedTimeSlotWidth = 0f
    private var cachedChartWidth = 0f
    private var cachedMaxChartHeight = 0
    
    // Pre-calculated fonts
    private val headerBoldFont = Font("JetBrains Mono", Font.BOLD, 11)
    private val headerPlainFont = Font("JetBrains Mono", Font.PLAIN, 11)
    private val legendFont = Font("JetBrains Mono", Font.PLAIN, 11)
    private val legendBoldFont = Font("JetBrains Mono", Font.BOLD, 11)
    private val tooltipFont = Font("JetBrains Mono", Font.BOLD, 12)
    private val tooltipPlainFont = Font("JetBrains Mono", Font.PLAIN, 11)
    private val emptyChartFont = Font("JetBrains Mono", Font.ITALIC, 14)
    private val timeFont = Font("Monospaced", Font.PLAIN, 12)
    private val dateBoldFont = Font("Monospaced", Font.BOLD, 12)
    
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
            val newStartTime = logs.lastOrNull()?.timestamp?.let { kotlinx.datetime.Instant.fromEpochMilliseconds(it) } ?: Clock.System.now()
            val newEndTime = logs.firstOrNull()?.timestamp?.let { kotlinx.datetime.Instant.fromEpochMilliseconds(it) } ?: Clock.System.now()
            val adjustedEndTime = if (newStartTime == newEndTime) newStartTime.plus(1.minutes) else newEndTime

            val durationTotal = newStartTime.until(adjustedEndTime, DateTimeUnit.MILLISECOND)
            val interval = durationTotal / (width.toLong() / widthDivision)
            val targetSize = (width.toLong() / widthDivision).toInt()

            // Check if we can do incremental update or need full recreation
            val needsFullRecreation = startTime != newStartTime || endTime != adjustedEndTime || timePoints.size != targetSize

            if (needsFullRecreation) {
                // Full recreation needed
                startTime = newStartTime
                endTime = adjustedEndTime
                
                timePoints.clear()
                (0 until targetSize).forEach { i ->
                    timePoints.add(TimePoint(startTime.plus(interval * i, DateTimeUnit.MILLISECOND)))
                }
                
                // Invalidate caches since time range changed
                invalidateCache()
            } else {
                // Incremental update - just reset counts and clear log entries
                timePoints.forEach { timePoint ->
                    timePoint.counts.fill(0)
                    timePoint.logEntries.clear()
                }
            }

            // Assign logs to time points efficiently
            logs.forEach { domain ->
                val level = domain.level
                val durationSinceStart = startTime.until(kotlinx.datetime.Instant.fromEpochMilliseconds(domain.timestamp), DateTimeUnit.MILLISECOND)
                val index = if (interval > 0) {
                    (durationSinceStart / interval).toInt().coerceIn(0, timePoints.size - 1)
                } else 0
                timePoints[index].incrementCount(level, domain)
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

    /** Invalidates caches when dimensions change */
    private fun invalidateCache() {
        cachedVisibleRange = null
    }

    /** Pre-calculates values that are used frequently in rendering */
    private fun updateRenderingCache(width: Int, height: Int) {
        val needsUpdate = lastRenderWidth != width || lastRenderHeight != height || cachedVisibleRange == null

        if (needsUpdate) {
            lastRenderWidth = width
            lastRenderHeight = height

            // Calculate dynamic left margin based on label width
            leftMargin = calculateLeftMargin()
            cachedChartWidth = (width - leftMargin - 30).toFloat() // 30px right margin
            cachedMaxChartHeight = height - 55 - 35
            cachedTimeSlotWidth = cachedChartWidth / timePoints.size.coerceAtLeast(1)

            cachedVisibleRange = 0 to (timePoints.size - 1)
        }

        // Initialize color cache if needed
        if (colorCache.isEmpty()) {
            allLogLevels.forEach { level ->
                colorCache[level] = getLevelColor(level)
            }
        }
    }

    /** Calculates the left margin needed to accommodate Y-axis labels */
    private fun calculateLeftMargin(): Int {
        if (timePoints.isEmpty()) return 30

        val font = Font("JetBrains Mono", Font.PLAIN, 11)
        val maxCount = timePoints.maxOfOrNull { it.getTotal() } ?: 0
        val maxLabel = (maxCount * 5 / 5).toString() // Get the maximum label value

        // Create a temporary graphics context to measure text width
        val tempImage = BufferedImage(1, 1, BufferedImage.TYPE_INT_RGB)
        val tempG2d = tempImage.createGraphics()
        tempG2d.font = font
        val metrics = tempG2d.fontMetrics
        val labelWidth = metrics.stringWidth(maxLabel)
        tempG2d.dispose()

        // Labels are drawn at x=5, so chart area should start after label + padding
        // labelWidth is the width of the text, so chart should start at 5 + labelWidth + 15 (more padding)
        return 5 + labelWidth + 15
    }
    
    /** Gets cached gradient for a level, creating if necessary */
    private fun getCachedGradient(level: LogLevel, x: Int, y: Int, height: Int): GradientPaint {
        val cacheKey = level
        return gradientCache.getOrPut(cacheKey) {
            val baseColor = colorCache[level] ?: getLevelColor(level)
            GradientPaint(
                x.toFloat(), y.toFloat(), baseColor.brighter(),
                x.toFloat(), (y + height).toFloat(), baseColor.darker()
            )
        }
    }

    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        // Pre-calculate values outside the lock to reduce lock time
        updateRenderingCache(width, height)
        
        lock.lock()
        try {
            // Reinitialize image if dimensions change or not yet initialized
            if (image == null || this.width != width || this.height != height) {
                g2d?.dispose()
                image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
                g2d = image!!.createGraphics().apply {
                    // Set rendering hints once during initialization
                    setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
                    setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON)
                    setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY)
                }
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

        // Draw enhanced gridlines and axis (adjusted for header height)
        drawEnhancedGridlines(currentScaleMax)
        
        // Draw main axis with better styling
        stroke = BasicStroke(2f)
        color = UiColors.defaultText.darker()
        drawLine(leftMargin, height - 35, width - 30, height - 35)
        stroke = BasicStroke(1f)

        // Track the last date to detect date changes
        var lastDate: String? = null

        // Iterate through all time points
        timePoints.forEachIndexed { index, timePoint ->
            val xPosBase = leftMargin + (index * cachedTimeSlotWidth).toInt()
            
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

                // Add date label using cached font
                font = dateBoldFont
                drawString(currentDate, xPosBase + 5, 70)

                // Reset to regular color for other elements
                color = Color.GRAY
            }

            // Draw regular time labels at intervals
            if (index % 25 == 0 || index == timePoints.size - 1) {
                font = timeFont
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
                        val barHeight = ((count.toFloat() / currentScaleMax) * cachedMaxChartHeight).toInt().coerceAtLeast(1)
                        val barWidth = cachedTimeSlotWidth.toInt() - 2
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
        drawLine(leftMargin, height - 35, width - 30, height - 35) // X-axis
        drawLine(leftMargin, 55, leftMargin, height - 35) // Y-axis
        stroke = BasicStroke(1f)
        
        // Add "No Data" message with cached font
        font = emptyChartFont
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
        val startDateStr = dateFormatter.format(startTime.toJavaInstant())
        val endDateStr = dateFormatter.format(endTime.toJavaInstant())
        
        // First line - Time range
        font = headerBoldFont
        color = UiColors.defaultText.brighter()
        val timeRangeText = if (startDateStr == endDateStr) {
            "$startDateStr ${timeFormatter.format(startTime.toJavaInstant())} - ${timeFormatter.format(endTime.toJavaInstant())}"
        } else {
            "$startDateStr ${timeFormatter.format(startTime.toJavaInstant())} - $endDateStr ${timeFormatter.format(endTime.toJavaInstant())}"
        }
        drawString(timeRangeText, 10, 18)
        
        // Second line - Duration and Statistics
        font = headerPlainFont
        color = UiColors.defaultText
        val duration = endTime.minus(startTime)
        val durationText = "Duration: $duration"
        drawString(durationText, 10, 35)
        
        // Right side - Statistics on second line
        font = headerBoldFont
        color = UiColors.teal
        val statsText = "Total: $totalMessages | Avg: %.1f/s".format(avgMps)
        val statsWidth = fontMetrics.stringWidth(statsText)
        drawString(statsText, width - statsWidth - 10, 35)
    }
    

    private fun Graphics2D.drawEnhancedGridlines(maxCount: Int) {
        // Draw subtle grid lines
        stroke = BasicStroke(1f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 0f, floatArrayOf(2f, 4f), 0f)
        color = Color(UiColors.defaultText.red, UiColors.defaultText.green, UiColors.defaultText.blue, 30)
        
        (1..5).forEach { i ->
            val y = height - 35 - ((i.toFloat() / 5) * (height - 105)).toInt()
            drawLine(leftMargin, y, width - 30, y)
        }
        
        // Draw Y-axis labels with better styling
        stroke = BasicStroke(1f)
        color = Color.GRAY  // Grey color like date labels
        font = Font("JetBrains Mono", Font.PLAIN, 11)

        (1..5).forEach { i ->
            val y = height - 35 - ((i.toFloat() / 5) * (height - 105)).toInt()
            val value = (maxCount * i / 5).toString()
            drawString(value, 5, y + 4)
        }
    }
    
    private fun Graphics2D.drawEnhancedBar(x: Int, y: Int, width: Int, height: Int, level: LogLevel, count: Int, timePoint: TimePoint) {
        val baseColor = colorCache[level] ?: getLevelColor(level)
        
        // Use cached gradient
        val gradient = getCachedGradient(level, x, y, height)
        
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
            
            // Draw tooltip content with cached fonts
            font = tooltipFont
            color = UiColors.defaultText
            
            val timeStr = timeFormatter.format(tooltip.timePoint.time.toJavaInstant())
            drawString("Time: $timeStr", tooltipX + padding, tooltipY + padding + 15)
            
            color = colorCache[tooltip.level] ?: getLevelColor(tooltip.level)
            drawString("${tooltip.level.name}: ${tooltip.count}", tooltipX + padding, tooltipY + padding + 35)
            
            color = UiColors.defaultText.darker()
            font = tooltipPlainFont
            drawString("Total: ${tooltip.timePoint.getTotal()}", tooltipX + padding, tooltipY + padding + 55)
            
            // Add percentage information
            val percentage = if (tooltip.timePoint.getTotal() > 0) {
                (tooltip.count.toFloat() / tooltip.timePoint.getTotal() * 100)
            } else 0f
            drawString("Percentage: %.1f%%".format(percentage), tooltipX + padding, tooltipY + padding + 70)
        }
    }

    private fun Graphics2D.drawLegend(levels: List<LogLevel>) {
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
                val selectionColor = colorCache[level] ?: getLevelColor(level)
                color = Color(selectionColor.red, selectionColor.green, selectionColor.blue, 60)
                fillRoundRect(boxX - 4, boxY - 4, labelWidth - 2, 18, 6, 6)
                
                color = Color(selectionColor.red, selectionColor.green, selectionColor.blue, 120)
                stroke = BasicStroke(1.5f)
                drawRoundRect(boxX - 4, boxY - 4, labelWidth - 2, 18, 6, 6)
                stroke = BasicStroke(1f)
            }

            // Enhanced color indicator with gradient
            val levelColor = colorCache[level] ?: getLevelColor(level)
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
            
            // Enhanced text rendering with cached fonts
            color = if (isSelected) UiColors.defaultText.brighter() else UiColors.defaultText.darker()
            font = if (isSelected) legendBoldFont else legendFont
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
        lastMouseX = e.x
        lastMouseY = e.y
    }
    
    override fun mouseReleased(e: MouseEvent) {
        // Check if we clicked on a bar in the chart area
        if (e.y >= 55 && e.y <= height - 35 && e.x >= leftMargin && e.x <= width - 30) {
            handleBarClick(e.x, e.y)
        }
    }
    
    private fun handleBarClick(mouseX: Int, mouseY: Int) {
        if (timePoints.isEmpty()) return
        
        // Calculate which time point was clicked
        val chartX = mouseX - leftMargin
        val timeIndex = (chartX / cachedTimeSlotWidth).toInt().coerceIn(0, timePoints.size - 1)
        
        // Pass the time index to the parent so it can calculate the correct offset
        // by counting events to the right of this bar and adding to current offset
        onBarClicked?.invoke(timeIndex)
    }
    override fun mouseEntered(e: MouseEvent) {
        mouseInside = true
    }

    override fun mouseExited(e: MouseEvent) {
        mouseInside = false
    }

    override fun mouseWheelMoved(e: MouseWheelEvent) {

    }
    
    override fun mouseDragged(e: MouseEvent) {
        lastMouseX = e.x
        lastMouseY = e.y
    }
    override fun mouseMoved(e: MouseEvent) {
        // Early exit if mouse is outside chart area
        val mouseX = e.x - leftMargin
        val mouseY = e.y
        
        if (mouseX < 0 || mouseX >= width - 60 || mouseY < 55 || mouseY > height - 35) {
            hoveredBar = null
            tooltipInfo = null
            return
        }
        
        // Calculate which time point is being hovered
        if (timePoints.isEmpty()) {
            hoveredBar = null
            tooltipInfo = null
            return
        }
        
        val timeIndex = (mouseX / cachedTimeSlotWidth).toInt().coerceIn(0, timePoints.size - 1)
        val timePoint = timePoints.getOrNull(timeIndex)
        
        if (timePoint == null) {
            hoveredBar = null
            tooltipInfo = null
            return
        }
        
        // Find which level bar is being hovered - optimized loop
        var currentHeight = 0
        var hoveredLevel: LogLevel? = null
        val selectedLevels = State.levels.get() // Cache the set lookup
        
        for (level in allLogLevels.reversed()) {
            if (level in selectedLevels) {
                val count = timePoint.getCount(level)
                if (count > 0) {
                    val barHeight = ((count.toFloat() / currentScaleMax) * cachedMaxChartHeight).toInt().coerceAtLeast(1)
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
    }
    
    /** Provides access to time points for offset calculation */
    fun getTimePoints(): List<TimePoint> = timePoints.toList()
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