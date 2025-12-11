package widgets

import ComponentOwn
import SlidePanel
import State
import app.Channels
import app.DomainLine
import app.QueryChanged
import app.ResultChanged
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.trySendBlocking
import util.UiColors
import java.awt.EventQueue
import java.awt.Font
import java.awt.Graphics2D
import java.awt.event.*
import java.awt.image.BufferedImage
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.math.max

class ScrollableList(
    private val panel: SlidePanel,
    x: Int,
    y: Int,
    width: Int,
    height: Int,
    private val inputTextLine: InputTextLine
) : ComponentOwn(), KeyListener, MouseListener, MouseWheelListener, MouseMotionListener {

    private var selectedTextRange: IntRange? = null
    private var indexOffset = 0

    // Lifecycle management
    private val scope = CoroutineScope(Dispatchers.Default + Job())
    private var isRunning = AtomicBoolean(true)
    private var follow = true
    private var lastUpdate = 0L

    // Constants
    private val HEADER_OFFSET = 7
    private val BOTTOM_MARGIN = 15
    private val MIN_CHART_HEIGHT = 80

    // Chart
    private var chartHeight = 100
    private val logLevelChart = LogLevelChart(
        x = x, y = 0, width = width, height = 100,
        onLevelsChanged = { updateResults() },
        onBarClicked = { offset -> handleBarClicked(offset) }
    )

    // Rendering
    private var lineList = listOf<LineItem>()
    private val rowHeight = 12
    private var rowHeightCurrent = 12
    private var image: BufferedImage? = null
    private lateinit var g2d: Graphics2D

    // Input state
    private var mouseposX = 0
    private var mouseposY = 0
    private var mouseInsideComponent = false

    // Font metrics - Default to rowHeight to prevent division by zero before init
    private var charHeight = rowHeight

    init {
        this.x = x
        this.y = y
        this.height = height
        this.width = width

        startMessageConsumer()
        startUiScheduler()
    }

    /**
     * Replaces the raw Thread. Consumes Kafka messages.
     */
    private fun startMessageConsumer() {
        scope.launch {
            while (isActive && isRunning.get()) {
                when (val msg = Channels.kafkaCmdGuiChannel.take()) { // Suspending receive
                    is ResultChanged -> {
                        if (msg.chartResult.isNotEmpty()) {
                            logLevelChart.updateChart(msg.chartResult)
                        }
                        updateResults(msg.result)
                    }
                    else -> {}
                }
            }
        }
    }

    /**
     * Replaces the ScheduledExecutor. Checks for state changes to trigger repaints.
     */
    private fun startUiScheduler() {
        scope.launch {
            while (isActive && isRunning.get()) {
                if (State.changedAt.get() > lastUpdate && follow) {
                    updateResults()
                    panel.repaint()
                }
                delay(16)
            }
        }
    }

    // Call this when the component is destroyed to stop threads
    fun dispose() {
        isRunning.set(false)
        scope.cancel()
        if (::g2d.isInitialized) g2d.dispose()
    }

    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        // Detect resize or move
        if (this.width != width || this.height != height || rowHeight != rowHeightCurrent || this.x != x || this.y != y || image == null) {

            if (::g2d.isInitialized) g2d.dispose()

            this.width = width
            this.height = height.coerceAtLeast(1)
            this.x = x
            this.y = y
            rowHeightCurrent = rowHeight

            // Recreate Image
            this.image = BufferedImage(
                this.width.coerceAtLeast(1),
                this.height.coerceAtLeast(1),
                BufferedImage.TYPE_INT_RGB
            )
            this.g2d = this.image!!.createGraphics()

            // Font setup
            g2d.font = loadFontFromResources(rowHeight.toFloat())
            val metrics = g2d.fontMetrics
            val maxBounds = metrics.getMaxCharBounds(g2d)
            charHeight = maxBounds.height.toInt().coerceAtLeast(1) // Safety

            // Recalculate layout
            chartHeight = (height * 0.2).toInt().coerceAtLeast(MIN_CHART_HEIGHT)
            rebuildLineItems()

            indexOffset = 0
            updateResults()
        }

        val g = g2d // Local access

        // Clear background
        g.color = UiColors.background
        g.fillRect(0, 0, width, height)

        // Draw Chart
        g.drawImage(logLevelChart.display(width, chartHeight, x, y), x, y, width, chartHeight, null)

        // Draw Lines
        g.color = UiColors.magenta
        paintLineItem(g)

        return image!!
    }

    override fun repaint(componentOwn: ComponentOwn) {
        // Safe unwrap or default
        val img = componentOwn.display(componentOwn.width, componentOwn.height, componentOwn.x, componentOwn.y)
        g2d.drawImage(
            img, componentOwn.x, componentOwn.y, componentOwn.width, componentOwn.height, null
        )
        panel.repaint(componentOwn.x, componentOwn.y, componentOwn.width, componentOwn.height)
    }

    private fun paintLineItem(g: Graphics2D) {
        lineList.forEach {
            g.drawImage(it.display(it.width, it.height, it.x, it.y), it.x, it.y, it.width, it.height, null)
        }
    }

    private fun calculateVisibleLines(): Int {
        if (charHeight == 0) return 0
        val availableHeight = height - chartHeight - BOTTOM_MARGIN
        return max(0, availableHeight / charHeight)
    }

    private fun rebuildLineItems() {
        val visibleLines = calculateVisibleLines()
        // Only rebuild if size changed significantly
        if (lineList.size != visibleLines + 1) {
            lineList = (0..visibleLines).map { index ->
                LineItem(
                    parent = this,
                    inputTextLine = inputTextLine,
                    x = x,
                    y = chartHeight + (charHeight * index),
                    width = width,
                    height = charHeight
                )
            }
        }
    }

    // --- Key Handling ---

    override fun keyPressed(e: KeyEvent) {
        // Prevent division by zero if key pressed before first paint
        val pageJump = if (charHeight > 0) height / charHeight else 10

        when (e.keyCode) {
            KeyEvent.VK_PAGE_UP -> {
                indexOffset += pageJump
                modifyOffsetAndSync(0) // 0 just triggers check
            }
            KeyEvent.VK_UP -> {
                indexOffset += 1
                modifyOffsetAndSync(0)
            }
            KeyEvent.VK_PAGE_DOWN -> {
                indexOffset -= pageJump
                modifyOffsetAndSync(0)
            }
            KeyEvent.VK_DOWN -> {
                indexOffset -= 1
                modifyOffsetAndSync(0)
            }
            KeyEvent.VK_ENTER -> {
                indexOffset = 0
                State.lock.set(0)
                modifyOffsetAndSync(0)
            }
        }
        panel.repaint()
    }

    private fun modifyOffsetAndSync(delta: Int) {
        indexOffset = ensureIndexOffset(indexOffset + delta)
        updateResults()
        setFollow()
    }

    // --- Mouse Handling ---

    override fun mouseClicked(e: MouseEvent) {
        if (e.y < chartHeight) {
            logLevelChart.mouseClicked(e)
        } else {
            lineList.firstOrNull { mouseInside(e, it) }?.mouseClicked(e)
            updateResults()
        }
    }

    override fun mousePressed(e: MouseEvent) {
        if (e.y < chartHeight) {
            logLevelChart.mousePressed(e)
        } else {
            mouseposX = e.x - x
            mouseposY = e.y - y - HEADER_OFFSET
            selectedTextRange = null
        }
        panel.repaint()
    }

    override fun mouseReleased(e: MouseEvent) {
        if (e.y < chartHeight) logLevelChart.mouseReleased(e)
    }

    override fun mouseEntered(e: MouseEvent) {
        mouseInsideComponent = true
        if (e.y < chartHeight) {
            logLevelChart.mouseEntered(e)
        } else {
            lineList.firstOrNull { mouseInside(e, it) }?.mouseEntered(e)
        }
    }

    override fun mouseExited(e: MouseEvent) {
        mouseInsideComponent = false
        if (e.y < chartHeight) logLevelChart.mouseExited(e)
    }

    override fun mouseWheelMoved(e: MouseWheelEvent) {
        if (e.y < chartHeight) {
            logLevelChart.mouseWheelMoved(e)
            return
        }

        if (e.isShiftDown) {
            // Horizontal/Fast scroll logic
            val delta = -((e.wheelRotation * e.scrollAmount) * 1024).toLong()
            State.lock.getAndAdd(delta)
            State.lock.set(State.lock.get().coerceAtLeast(0))

            indexOffset = ensureIndexOffset(indexOffset)
            updateResults()
            setFollow()
            panel.repaint()
            return
        }

        val delta = -(e.wheelRotation * e.scrollAmount)
        indexOffset += delta
        indexOffset = ensureIndexOffset(indexOffset)

        // highlightWordMouseOver = null // (Was unused in original snippet, assuming intended)
        updateResults()
        setFollow()
        panel.repaint()
    }

    override fun mouseDragged(e: MouseEvent) {
        if (e.y < chartHeight) {
            logLevelChart.mouseDragged(e)
        } else {
            mouseposX = e.x - x
            mouseposY = e.y - y - HEADER_OFFSET
        }
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y

        if (e.y < chartHeight) {
            logLevelChart.mouseMoved(e)
        } else {
            // Optimized finding logic
            val itemUnderMouse = lineList.firstOrNull { mouseInside(e, it) }
            itemUnderMouse?.mouseMoved(e)
            itemUnderMouse?.mouseEntered(e)

            lineList.forEach {
                if (it !== itemUnderMouse) it.mouseExited(e)
            }
        }
    }

    // --- Logic Helpers ---

    private fun setFollow() {
        follow = (indexOffset == 0)
    }

    private fun ensureIndexOffset(i: Int): Int {
        return i.coerceAtLeast(0) // Int.MAX_VALUE is implicit upper bound of Int
    }

    // Fixed coordinate calculation: check bounds of X relative to component X
    private fun mouseInside(e: MouseEvent, it: ComponentOwn): Boolean {
        // Assuming ComponentOwn.x is absolute or relative to parent.
        // If 'it.x' is relative to ScrollableList:
        return e.x >= it.x && e.x <= (it.x + it.width) &&
                e.y >= it.y && e.y <= (it.y + it.height)
    }

    private fun updateResults() {
        val visibleLen = calculateVisibleLines()
        State.offset.set(indexOffset)
        State.length.set(visibleLen)

        Channels.searchChannel.trySendBlocking(
            QueryChanged(
                query = inputTextLine.text,
                length = visibleLen,
                offset = indexOffset,
            )
        )
    }

    private fun updateResults(result: List<DomainLine>) {
        // Ensure UI elements exist for the data
        rebuildLineItems()

        EventQueue.invokeLater {
            lineList.forEach { it.setText("") } // Clear all first
            result.forEachIndexed { i, item ->
                if (i < lineList.size) {
                    lineList[i].setLogJson(item)
                }
            }
            lastUpdate = System.nanoTime()
        }
    }

    private fun handleBarClicked(clickedTimeIndex: Int) {
        val chartTimePoints = logLevelChart.getTimePoints()
        if (clickedTimeIndex >= chartTimePoints.size) return

        var eventsToRight = 0
        for (i in clickedTimeIndex until chartTimePoints.size) {
            eventsToRight += chartTimePoints[i].getTotal()
        }

        indexOffset = eventsToRight
        follow = false
        updateResults()
        panel.repaint()
    }

    // Unused overrides
    override fun keyTyped(e: KeyEvent) {}
    override fun keyReleased(e: KeyEvent) {}
}

fun loadFontFromResources(size: Float = 14f): Font {
    // Keep this function as is, or move to a separate utility file
    val stream = ClassLoader.getSystemResourceAsStream("JetBrainsMono-Regular.ttf")
        ?: error("Font not found at JetBrainsMono-Regular.ttf")
    return Font.createFont(Font.TRUETYPE_FONT, stream).deriveFont(size)
}