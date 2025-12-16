@file:OptIn(kotlin.time.ExperimentalTime::class)

package widgets

import ComponentOwn
import LogLevel
import State
import app.DomainLine
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
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.max
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.DurationUnit
import kotlin.time.toJavaInstant

/**
 * Thread-safe, snapshot-based chart renderer.
 * updateChart() may be called from a background thread.
 * display()/mouse events are expected on the EDT.
 */
class LogLevelChart(
    x: Int, y: Int, width: Int, height: Int,
    private val onLevelsChanged: (() -> Unit)? = null,
    private val onBarClicked: ((indexOffset: Int) -> Unit)? = null
) : ComponentOwn() {

    // ---------- Immutable snapshot model ----------

    data class TimePoint(
        val time: kotlin.time.Instant,
        val counts: IntArray, // immutable after publish
        val total: Int
    ) {
        fun getCount(level: LogLevel): Int = counts[level.ordinal]
    }

    private data class ChartState(
        val startTime: kotlin.time.Instant,
        val endTime: kotlin.time.Instant,
        val intervalMs: Long,
        val points: List<TimePoint>,
        val scaleMax: Int
    ) {
        companion object {
            val EMPTY = ChartState(
                startTime = Clock.System.now(),
                endTime = Clock.System.now(),
                intervalMs = 1L,
                points = emptyList(),
                scaleMax = 100
            )
        }
    }

    // Layout used by mouse handlers (computed in display)
    private data class Layout(
        val width: Int,
        val height: Int,
        val leftMargin: Int,
        val rightMargin: Int,
        val headerBottom: Int,
        val xAxisBottomPad: Int,
        val chartTop: Int,
        val chartBottom: Int,
        val chartHeight: Int,
        val timeSlotWidth: Float
    ) {
        companion object {
            val EMPTY = Layout(
                width = 1,
                height = 1,
                leftMargin = 30,
                rightMargin = 30,
                headerBottom = 55,
                xAxisBottomPad = 35,
                chartTop = 55,
                chartBottom = 1 - 35,
                chartHeight = 1,
                timeSlotWidth = 1f
            )
        }
    }

    // ---------- State (thread-safe publication) ----------

    private val stateRef = AtomicReference(ChartState.EMPTY)
    private val layoutRef = AtomicReference(Layout.EMPTY)

    // updateChart synchronization + scale hysteresis state
    private val updateLock = Any()
    private var currentScaleMax = 100
    private var scaleLastChangedTime = Instant.now()
    private var pendingScaleMax = 0

    // Keep last logs so we can re-bin on resize (optional but helpful)
    @Volatile
    private var lastLogs: List<DomainLine> = emptyList()
    @Volatile
    private var lastBinnedWidth: Int = 0

    // ---------- UI / caches (EDT-only) ----------

    private var image: BufferedImage? = null
    private var g2d: Graphics2D? = null

    private val allLogLevels = LogLevel.entries.toTypedArray().toList()
    private val colorCache: Map<LogLevel, Color> = allLogLevels.associateWith { getLevelColor(it) }

    // Legend hit boxes (EDT-only mutation, but published immutably for safety)
    @Volatile
    private var levelHitBoxes: Map<LogLevel, Rectangle> = emptyMap()

    // Hover/tooltip are updated by mouseMoved (EDT), read by display (EDT). Volatile for safety.
    private data class Hover(val index: Int, val level: LogLevel)
    private data class TooltipInfo(
        val x: Int,
        val y: Int,
        val timePoint: TimePoint,
        val level: LogLevel,
        val count: Int
    )

    @Volatile
    private var hoveredBar: Hover? = null
    @Volatile
    private var tooltipInfo: TooltipInfo? = null

    // Fonts
    private val headerBoldFont = Font("JetBrains Mono", Font.BOLD, 11)
    private val headerPlainFont = Font("JetBrains Mono", Font.PLAIN, 11)
    private val legendFont = Font("JetBrains Mono", Font.PLAIN, 11)
    private val legendBoldFont = Font("JetBrains Mono", Font.BOLD, 11)
    private val tooltipFont = Font("JetBrains Mono", Font.BOLD, 12)
    private val tooltipPlainFont = Font("JetBrains Mono", Font.PLAIN, 11)
    private val emptyChartFont = Font("JetBrains Mono", Font.ITALIC, 14)
    private val timeFont = Font("Monospaced", Font.PLAIN, 12)
    private val dateBoldFont = Font("Monospaced", Font.BOLD, 12)

    private val widthDivision = 6
    private val timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss").withZone(ZoneId.of("UTC"))
    private val dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneId.of("UTC"))

    // Geometry constants (match your existing drawing)
    private val HEADER_BOTTOM = 55
    private val X_AXIS_BOTTOM_PAD = 35
    private val RIGHT_MARGIN = 30

    init {
        this.x = x
        this.y = y
        this.width = width
        this.height = height
    }

    // ---------- Public API ----------

    fun updateChart(logs: List<DomainLine>) {
        if (logs.isEmpty()) {
            lastLogs = emptyList()
            stateRef.set(ChartState.EMPTY)
            return
        }

        // Cache logs reference so we can re-bin on resize
        lastLogs = logs

        val w = max(1, this.width) // may be 0 before first display

        synchronized(updateLock) {
            val newState = buildState(logs, w)
            stateRef.set(newState)
            lastBinnedWidth = w
        }
    }

    fun getTimePoints(): List<TimePoint> = stateRef.get().points

    // ---------- Rendering ----------

    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        // Update component geometry (EDT)
        val dimChanged = (image == null || this.width != width || this.height != height)
        if (dimChanged) {
            g2d?.dispose()
            image = BufferedImage(max(1, width), max(1, height), BufferedImage.TYPE_INT_RGB)
            g2d = image!!.createGraphics().apply {
                setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
                setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON)
                setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY)
            }
        }

        this.width = max(1, width)
        this.height = max(1, height)
        this.x = x
        this.y = y

        // If width changed, re-bin using lastLogs (prevents mismatch after resize)
        val logsSnapshot = lastLogs
        if (logsSnapshot.isNotEmpty() && width != lastBinnedWidth) {
            synchronized(updateLock) {
                val newState = buildState(logsSnapshot, max(1, width))
                stateRef.set(newState)
                lastBinnedWidth = max(1, width)
            }
        }

        val g = g2d!!
        val state = stateRef.get()

        // Clear background
        g.color = UiColors.background
        g.fillRect(0, 0, this.width, this.height)

        // Compute layout based on current Graphics2D metrics
        val layout = computeLayout(g, state, this.width, this.height)
        layoutRef.set(layout)

        // Draw chart
        g.drawChart(state, layout)
        g.drawTooltip(layout)

        return image!!
    }

    override fun repaint(componentOwn: ComponentOwn) {
        // no-op here (your parent calls display and paints the returned image)
    }

    // ---------- Layout helpers ----------

    private fun computeLayout(g: Graphics2D, state: ChartState, width: Int, height: Int): Layout {
        val leftMargin = calculateLeftMargin(g, state.scaleMax)
        val chartTop = HEADER_BOTTOM
        val chartBottom = height - X_AXIS_BOTTOM_PAD
        val chartHeight = max(1, chartBottom - chartTop)
        val pointsCount = max(1, state.points.size)
        val chartWidth = max(1, width - leftMargin - RIGHT_MARGIN)
        val timeSlotWidth = chartWidth.toFloat() / pointsCount

        return Layout(
            width = width,
            height = height,
            leftMargin = leftMargin,
            rightMargin = RIGHT_MARGIN,
            headerBottom = HEADER_BOTTOM,
            xAxisBottomPad = X_AXIS_BOTTOM_PAD,
            chartTop = chartTop,
            chartBottom = chartBottom,
            chartHeight = chartHeight,
            timeSlotWidth = timeSlotWidth
        )
    }

    private fun calculateLeftMargin(g: Graphics2D, scaleMax: Int): Int {
        val font = Font("JetBrains Mono", Font.PLAIN, 11)
        g.font = font
        val label = scaleMax.toString()
        val labelWidth = g.fontMetrics.stringWidth(label)
        // labels are drawn at x=5; chart starts after label + padding
        return 5 + labelWidth + 15
    }

    // ---------- Build snapshot ----------

    private fun buildState(logs: List<DomainLine>, width: Int): ChartState {
        // Time range (logs assumed newest -> oldest, as in your code)
        val newStartTime = logs.lastOrNull()?.timestamp?.let { kotlin.time.Instant.fromEpochMilliseconds(it) }
            ?: Clock.System.now()
        val newEndTime = logs.firstOrNull()?.timestamp?.let { kotlin.time.Instant.fromEpochMilliseconds(it) }
            ?: Clock.System.now()
        val adjustedEndTime = if (newStartTime == newEndTime) newStartTime else newEndTime

        val binCount = max(1, width / widthDivision)

        val durationMs = max(
            1L,
            adjustedEndTime.minus(newStartTime).toLong(DurationUnit.MILLISECONDS)
        )
        val intervalMs = max(1L, durationMs / binCount.toLong())

        // Init bins
        val points = Array(binCount) { i ->
            val t = newStartTime.plus((intervalMs * i).milliseconds)
            TimePoint(
                time = t,
                counts = IntArray(LogLevel.entries.size),
                total = 0
            )
        }

        // Fill counts (mutate local arrays only)
        for (domain in logs) {
            val level = domain.level
            val ts = kotlin.time.Instant.fromEpochMilliseconds(domain.timestamp)
            val sinceStart = ts.minus(newStartTime).toLong(DurationUnit.MILLISECONDS)
            val idx = (sinceStart / intervalMs).toInt().coerceIn(0, binCount - 1)
            points[idx].counts[level.ordinal]++
        }

        // Compute totals + max total
        var maxTotal = 0
        val finalized = ArrayList<TimePoint>(binCount)
        for (p in points) {
            val total = p.counts.sum()
            if (total > maxTotal) maxTotal = total
            finalized.add(TimePoint(time = p.time, counts = p.counts, total = total))
        }

        // Update scale with hysteresis (protected by updateLock)
        updateScale(maxTotal)
        return ChartState(
            startTime = newStartTime,
            endTime = adjustedEndTime,
            intervalMs = intervalMs,
            points = finalized,
            scaleMax = currentScaleMax
        )
    }

    private fun updateScale(newMaxTotal: Int) {
        val now = Instant.now()
        val timeSinceLastChange = Duration.between(scaleLastChangedTime, now).seconds

        when {
            newMaxTotal > currentScaleMax * 1.5 -> {
                pendingScaleMax = newMaxTotal
                if (timeSinceLastChange > 2 || newMaxTotal > currentScaleMax * 4) {
                    currentScaleMax = (newMaxTotal * 1.2).toInt().coerceAtLeast(5)
                    scaleLastChangedTime = now
                    pendingScaleMax = 0
                }
            }

            newMaxTotal < currentScaleMax / 3 && timeSinceLastChange > 1 -> {
                currentScaleMax = max(newMaxTotal * 2, 5)
                scaleLastChangedTime = now
            }
        }
    }

    // ---------- Drawing ----------

    private fun Graphics2D.drawChart(state: ChartState, layout: Layout) {
        if (state.points.isEmpty()) {
            drawEmptyChart(layout)
            return
        }

        drawEnhancedGridlines(layout, state.scaleMax)

        // X-axis
        stroke = BasicStroke(2f)
        color = UiColors.defaultText.darker()
        drawLine(layout.leftMargin, layout.chartBottom, layout.width - layout.rightMargin, layout.chartBottom)
        stroke = BasicStroke(1f)

        // Bars + labels
        var lastDate: String? = null
        val selectedLevels = State.levels.get()
        val points = state.points

        for (index in points.indices) {
            val tp = points[index]
            val xPosBase = layout.leftMargin + (index * layout.timeSlotWidth).toInt()

            val javaInstant = tp.time.toJavaInstant()
            val currentDate = dateFormatter.format(javaInstant)

            if (lastDate != currentDate) {
                // date change line
                val originalStroke = stroke
                stroke = BasicStroke(2f)
                color = UiColors.teal
                drawLine(xPosBase, layout.chartTop, xPosBase, layout.chartBottom)
                stroke = originalStroke

                font = dateBoldFont
                color = Color.GRAY
                drawString(currentDate, xPosBase + 5, layout.chartTop + 15)
            }

            // time labels
            if (index % 25 == 0 || index == points.lastIndex) {
                font = timeFont
                color = Color.GRAY
                drawString(timeFormatter.format(javaInstant), xPosBase, layout.height - 15)
            }

            lastDate = currentDate

            // stacked bars
            var currentHeight = 0
            for (level in allLogLevels) {
                if (level !in selectedLevels) continue
                val count = tp.getCount(level)
                if (count <= 0) continue

                val barHeight = ((count.toFloat() / state.scaleMax) * layout.chartHeight)
                    .toInt()
                    .coerceAtLeast(1)

                val barWidth = (layout.timeSlotWidth.toInt() - 2).coerceAtLeast(1)
                val barX = xPosBase
                val barY = layout.chartBottom - currentHeight - barHeight

                val adjustedBarY = barY.coerceAtLeast(layout.chartTop)
                val adjustedBarHeight = if (adjustedBarY > barY) barHeight - (adjustedBarY - barY) else barHeight

                if (adjustedBarHeight > 0) {
                    drawEnhancedBar(
                        barX, adjustedBarY, barWidth, adjustedBarHeight,
                        level = level,
                        index = index
                    )
                }

                currentHeight += barHeight
            }
        }

        // Header stats
        val totalMessages = points.sumOf { it.total }
        val durationSecs = state.endTime.minus(state.startTime).toInt(DurationUnit.SECONDS).coerceAtLeast(1)
        val avgMps = totalMessages.toFloat() / durationSecs
        drawEnhancedHeader(state, totalMessages, avgMps)

        // Legend
        drawLegend()
    }

    private fun Graphics2D.drawEmptyChart(layout: Layout) {
        color = UiColors.background
        fillRect(0, 0, layout.width, layout.height)

        drawEnhancedGridlines(layout, 100)

        stroke = BasicStroke(2f)
        color = UiColors.defaultText.darker()
        drawLine(layout.leftMargin, layout.chartBottom, layout.width - layout.rightMargin, layout.chartBottom)
        drawLine(layout.leftMargin, layout.chartTop, layout.leftMargin, layout.chartBottom)
        stroke = BasicStroke(1f)

        font = emptyChartFont
        color = UiColors.defaultText.darker()
        val message = "No log data available"
        val messageWidth = fontMetrics.stringWidth(message)
        drawString(message, (layout.width - messageWidth) / 2, layout.height / 2)

        drawLegend()
    }

    private fun Graphics2D.drawEnhancedHeader(state: ChartState, totalMessages: Int, avgMps: Float) {
        val headerHeight = 50
        val gradient = GradientPaint(
            0f,
            0f,
            Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200),
            0f,
            headerHeight.toFloat(),
            Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 100)
        )
        paint = gradient
        fillRect(0, 0, width, headerHeight)

        color = UiColors.defaultText.darker()
        stroke = BasicStroke(1f)
        drawLine(0, headerHeight, width, headerHeight)

        val startDateStr = dateFormatter.format(state.startTime.toJavaInstant())
        val endDateStr = dateFormatter.format(state.endTime.toJavaInstant())

        font = headerBoldFont
        color = UiColors.defaultText.brighter()

        val timeRangeText =
            if (startDateStr == endDateStr) {
                "$startDateStr ${timeFormatter.format(state.startTime.toJavaInstant())} - ${timeFormatter.format(state.endTime.toJavaInstant())}"
            } else {
                "$startDateStr ${timeFormatter.format(state.startTime.toJavaInstant())} - $endDateStr ${
                    timeFormatter.format(
                        state.endTime.toJavaInstant()
                    )
                }"
            }
        drawString(timeRangeText, 10, 18)

        font = headerPlainFont
        color = UiColors.defaultText
        val duration = state.endTime.minus(state.startTime)
        drawString("Duration: $duration", 10, 35)

        font = headerBoldFont
        color = UiColors.teal
        val statsText = "Total: $totalMessages | Avg: %.1f/s".format(avgMps)
        val statsWidth = fontMetrics.stringWidth(statsText)
        drawString(statsText, width - statsWidth - 10, 35)
    }

    private fun Graphics2D.drawEnhancedGridlines(layout: Layout, maxCount: Int) {
        // subtle grid
        stroke = BasicStroke(1f, BasicStroke.CAP_ROUND, BasicStroke.JOIN_ROUND, 0f, floatArrayOf(2f, 4f), 0f)
        color = Color(UiColors.defaultText.red, UiColors.defaultText.green, UiColors.defaultText.blue, 30)

        for (i in 1..5) {
            val y = layout.chartBottom - ((i.toFloat() / 5f) * layout.chartHeight).toInt()
            drawLine(layout.leftMargin, y, layout.width - layout.rightMargin, y)
        }

        // y labels
        stroke = BasicStroke(1f)
        color = Color.GRAY
        font = Font("JetBrains Mono", Font.PLAIN, 11)

        for (i in 1..5) {
            val y = layout.chartBottom - ((i.toFloat() / 5f) * layout.chartHeight).toInt()
            val value = (maxCount * i / 5).toString()
            drawString(value, 5, y + 4)
        }
    }

    private fun Graphics2D.drawEnhancedBar(
        x: Int,
        y: Int,
        width: Int,
        height: Int,
        level: LogLevel,
        index: Int
    ) {
        val baseColor = colorCache[level] ?: getLevelColor(level)
        val isHovered = hoveredBar?.let { it.index == index && it.level == level } == true

        if (isHovered) {
            val glowColor = Color(baseColor.red, baseColor.green, baseColor.blue, 100)
            paint = GradientPaint(
                (x - 2).toFloat(), (y - 2).toFloat(), glowColor,
                (x - 2).toFloat(), (y + height + 4).toFloat(), Color(0, 0, 0, 0)
            )
            fill(
                RoundRectangle2D.Float(
                    (x - 2).toFloat(),
                    (y - 2).toFloat(),
                    (width + 4).toFloat(),
                    (height + 4).toFloat(),
                    4f,
                    4f
                )
            )
        }

        // Gradient per bar (correct + still fast enough)
        paint = GradientPaint(
            x.toFloat(), y.toFloat(), baseColor.brighter(),
            x.toFloat(), (y + height).toFloat(), baseColor.darker()
        )
        fill(RoundRectangle2D.Float(x.toFloat(), y.toFloat(), width.toFloat(), height.toFloat(), 3f, 3f))

        color = baseColor.darker().darker()
        stroke = BasicStroke(0.5f)
        draw(RoundRectangle2D.Float(x.toFloat(), y.toFloat(), width.toFloat(), height.toFloat(), 3f, 3f))
        stroke = BasicStroke(1f)
    }

    private fun Graphics2D.drawTooltip(layout: Layout) {
        val tooltip = tooltipInfo ?: return

        val tooltipWidth = 200
        val tooltipHeight = 95
        val padding = 8

        var tooltipX = tooltip.x + 10
        var tooltipY = tooltip.y - tooltipHeight - 10

        if (tooltipX + tooltipWidth > layout.width) tooltipX = tooltip.x - tooltipWidth - 10
        if (tooltipY < 0) tooltipY = tooltip.y + 20

        color = Color(0, 0, 0, 100)
        fillRoundRect(tooltipX + 2, tooltipY + 2, tooltipWidth, tooltipHeight, 8, 8)

        color = Color(40, 42, 46)
        fillRoundRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight, 8, 8)

        color = UiColors.defaultText.darker()
        stroke = BasicStroke(1f)
        drawRoundRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight, 8, 8)

        font = tooltipFont
        color = UiColors.defaultText
        val timeStr = timeFormatter.format(tooltip.timePoint.time.toJavaInstant())
        drawString("Time: $timeStr", tooltipX + padding, tooltipY + padding + 15)

        color = colorCache[tooltip.level] ?: getLevelColor(tooltip.level)
        drawString("${tooltip.level.name}: ${tooltip.count}", tooltipX + padding, tooltipY + padding + 35)

        color = UiColors.defaultText.darker()
        font = tooltipPlainFont
        drawString("Total: ${tooltip.timePoint.total}", tooltipX + padding, tooltipY + padding + 55)

        val percentage =
            if (tooltip.timePoint.total > 0) (tooltip.count.toFloat() / tooltip.timePoint.total * 100f) else 0f
        drawString("Percentage: %.1f%%".format(percentage), tooltipX + padding, tooltipY + padding + 70)
    }

    private fun Graphics2D.drawLegend() {
        val levels = allLogLevels
        val labelWidth = 90
        val totalWidth = labelWidth * levels.size
        val startX = (width - totalWidth) / 2

        val boxes = HashMap<LogLevel, Rectangle>(levels.size)

        for ((index, level) in levels.withIndex()) {
            val boxX = startX + (index * labelWidth)
            val boxY = 15
            val isSelected = level in State.levels.get()

            if (isSelected) {
                val selectionColor = colorCache[level] ?: getLevelColor(level)
                color = Color(selectionColor.red, selectionColor.green, selectionColor.blue, 60)
                fillRoundRect(boxX - 4, boxY - 4, labelWidth - 2, 18, 6, 6)

                color = Color(selectionColor.red, selectionColor.green, selectionColor.blue, 120)
                stroke = BasicStroke(1.5f)
                drawRoundRect(boxX - 4, boxY - 4, labelWidth - 2, 18, 6, 6)
                stroke = BasicStroke(1f)
            }

            val levelColor = colorCache[level] ?: getLevelColor(level)
            paint = GradientPaint(
                boxX.toFloat(), boxY.toFloat(), levelColor.brighter(),
                boxX.toFloat(), (boxY + 12).toFloat(), levelColor.darker()
            )
            fillRoundRect(boxX, boxY, 12, 12, 3, 3)

            color = levelColor.darker()
            stroke = BasicStroke(0.5f)
            drawRoundRect(boxX, boxY, 12, 12, 3, 3)
            stroke = BasicStroke(1f)

            color = if (isSelected) UiColors.defaultText.brighter() else UiColors.defaultText.darker()
            font = if (isSelected) legendBoldFont else legendFont
            drawString(level.name, boxX + 18, boxY + 10)

            boxes[level] = Rectangle(boxX - 4, boxY - 4, labelWidth - 2, 18)
        }

        levelHitBoxes = boxes.toMap()
    }

    // ---------- Input handling ----------

    override fun mouseClicked(e: MouseEvent) {
        levelHitBoxes.entries.find { it.value.contains(e.point) }?.key?.let { level ->
            if (level in State.levels.get() && State.levels.get().size > 1) {
                val logLevels = State.levels.get()
                logLevels.remove(level)
                State.levels.set(logLevels)
            } else {
                State.levels.get().add(level)
            }
            onLevelsChanged?.invoke()
        }
    }

    override fun mousePressed(e: MouseEvent) {}

    override fun mouseReleased(e: MouseEvent) {
        val layout = layoutRef.get()
        if (e.y < layout.chartTop || e.y > layout.chartBottom) return
        if (e.x < layout.leftMargin || e.x > layout.width - layout.rightMargin) return
        handleBarClick(e.x, layout)
    }

    private fun handleBarClick(mouseX: Int, layout: Layout) {
        val state = stateRef.get()
        if (state.points.isEmpty()) return

        val chartX = mouseX - layout.leftMargin
        val timeIndex = (chartX / layout.timeSlotWidth).toInt().coerceIn(0, state.points.size - 1)
        onBarClicked?.invoke(timeIndex)
    }

    override fun mouseMoved(e: MouseEvent) {
        val layout = layoutRef.get()
        val state = stateRef.get()
        if (state.points.isEmpty()) {
            hoveredBar = null
            tooltipInfo = null
            return
        }

        // outside plot area?
        if (e.x < layout.leftMargin || e.x >= layout.width - layout.rightMargin || e.y < layout.chartTop || e.y > layout.chartBottom) {
            hoveredBar = null
            tooltipInfo = null
            return
        }

        val chartX = e.x - layout.leftMargin
        val timeIndex = (chartX / layout.timeSlotWidth).toInt().coerceIn(0, state.points.size - 1)
        val timePoint = state.points[timeIndex]

        // Determine hovered stack segment
        var currentHeight = 0
        var hoveredLevel: LogLevel? = null
        val selectedLevels = State.levels.get()

        // Use reversed order to match your original hover logic
        for (level in allLogLevels.asReversed()) {
            if (level !in selectedLevels) continue

            val count = timePoint.getCount(level)
            if (count <= 0) continue

            val barHeight = ((count.toFloat() / state.scaleMax) * layout.chartHeight).toInt().coerceAtLeast(1)
            val barY = layout.chartBottom - currentHeight - barHeight
            val adjustedBarY = barY.coerceAtLeast(layout.chartTop)
            val adjustedBarHeight = if (adjustedBarY > barY) barHeight - (adjustedBarY - barY) else barHeight

            val barTop = adjustedBarY
            val barBottom = adjustedBarY + adjustedBarHeight

            if (e.y in barTop..barBottom) {
                hoveredLevel = level
                tooltipInfo = TooltipInfo(e.x, e.y, timePoint, level, count)
                break
            }

            currentHeight += barHeight
        }

        hoveredBar = if (hoveredLevel != null) Hover(timeIndex, hoveredLevel) else null
        if (hoveredLevel == null) tooltipInfo = null
    }

    override fun mouseDragged(e: MouseEvent) {}
    override fun mouseWheelMoved(e: MouseWheelEvent) {}
    override fun mouseEntered(e: MouseEvent) {}
    override fun mouseExited(e: MouseEvent) {}

    override fun keyPressed(e: KeyEvent) {
        when (e.keyCode) {
            KeyEvent.VK_R -> {
                State.levels.get().clear()
                State.levels.get().addAll(allLogLevels)
                onLevelsChanged?.invoke()
            }
        }
    }

    override fun keyTyped(e: KeyEvent) {}
    override fun keyReleased(e: KeyEvent) {}
}

// Keep your existing colors
fun getLevelColor(level: LogLevel): Color = when (level) {
    LogLevel.INFO -> Color(76, 175, 80)
    LogLevel.WARN -> Color(255, 152, 0)
    LogLevel.DEBUG -> Color(96, 125, 139)
    LogLevel.ERROR -> Color(244, 67, 54)
    LogLevel.UNKNOWN -> Color(158, 158, 158)
    LogLevel.KAFKA -> Color(0, 188, 212)
}

fun getLevelColorLight(level: LogLevel): Color = when (level) {
    LogLevel.INFO -> Color(129, 199, 132)
    LogLevel.WARN -> Color(255, 183, 77)
    LogLevel.DEBUG -> Color(144, 164, 174)
    LogLevel.ERROR -> Color(239, 154, 154)
    LogLevel.UNKNOWN -> Color(189, 189, 189)
    LogLevel.KAFKA -> Color(77, 208, 225)
}