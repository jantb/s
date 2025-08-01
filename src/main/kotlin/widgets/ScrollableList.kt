package widgets

import ComponentOwn
import SlidePanel
import State
import app.Channels
import app.DomainLine
import app.QueryChanged
import app.ResultChanged
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.datetime.DateTimeUnit
import kotlinx.datetime.until
import util.UiColors
import java.awt.EventQueue
import java.awt.Font
import java.awt.Graphics2D
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class ScrollableList(
    private val panel: SlidePanel, x: Int, y: Int, width: Int, height: Int, private val inputTextLine: InputTextLine
) : ComponentOwn(), KeyListener, MouseListener, MouseWheelListener, MouseMotionListener {
    private var selectedTextRange: IntRange? = null
    private var highlightWordMouseOver: IntRange? = null
    private var indexOffset = 0
    private val scheduler = Executors.newScheduledThreadPool(1)
    private var follow = true
    private var lastUpdate = 0L

    // Chart for log levels
    private val logLevelChart = LogLevelChart(
        x = x, y = 0, width = width, height = 100,
        onLevelsChanged = { updateResults() },
        onBarClicked = { indexOffset -> handleBarClicked(indexOffset) }
    )
    private var chartHeight = 100

    init {
        this.x = x
        this.y = y
        this.height = height
        this.width = width
        start()
        scheduler.scheduleWithFixedDelay({
            if (State.changedAt.get() > lastUpdate && follow) {
                updateResults()
                panel.repaint()
            }
        }, 0, 16, TimeUnit.MILLISECONDS)
    }

    private var selectedLineIndex = 0

    private var rowHeight = 12
    private var rowHeightCurrent = 12
    private lateinit var image: BufferedImage
    private lateinit var g2d: Graphics2D

    private var mouseposX = 0
    private var mouseposY = 0
    private var mouseposXPressed = 0
    private var mouseposYPressed = 0
    private var mouseposXReleased = 0
    private var mouseposYReleased = 0
    private lateinit var maxCharBounds: Rectangle2D


    private fun start() {
        val thread = Thread {
            while (true) {
                when (val msg = Channels.kafkaCmdGuiChannel.take()) {
                    is ResultChanged -> {
                        // Use the chart result for the chart if available, otherwise use the regular result
                        // Update the chart with the chart data
                        if (msg.chartResult.isNotEmpty()) (logLevelChart.updateChart(msg.chartResult))
                        // Update the scrollable list with the regular result
                        updateResults(msg.result)
                    }

                    else -> {
                    }
                }
            }
        }
        thread.isDaemon = true
        thread.start()
    }

    private var lineList = listOf<LineItem>()
    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        if (this.width != width || this.height != height || rowHeight != rowHeightCurrent || this.x != x || this.y != y) {
            if (::g2d.isInitialized) {
                this.g2d.dispose()
            }
            if (height < 0) {
                this.height = 1
            } else {
                this.height = height
            }
            this.image = BufferedImage(
                width.coerceIn(1..Int.MAX_VALUE), this.height.coerceIn(1..Int.MAX_VALUE), BufferedImage.TYPE_INT_RGB
            )
            this.g2d = this.image.createGraphics()
            this.image
            this.width = width
            this.x = x
            this.y = y

            // Make chart height proportional to window height (20% of total height, minimum 80 pixels)
            chartHeight = (height * 0.2).toInt().coerceAtLeast(80)
            rowHeightCurrent = rowHeight
            if (::maxCharBounds.isInitialized) {
                val length = if (!::maxCharBounds.isInitialized || maxCharBounds.height.toInt() == 0) {
                    0
                } else {
                    // Adjust for chart height
                    ((height - chartHeight - maxCharBounds.height.toInt()) / maxCharBounds.height.toInt()) + 1
                }
                lineList = (0..length).map {
                    LineItem(
                        parent = this,
                        inputTextLine = inputTextLine,
                        x = x,
                        // Adjust y position to start below the chart
                        y = chartHeight + ((maxCharBounds.height.toInt()) * (it)),
                        width = width,
                        height = ((maxCharBounds.height.toInt()))
                    )
                }
            }
            g2d.font = loadFontFromResources(rowHeight.toFloat())
            indexOffset = 0
            updateResults()
        }
        //Clear
        g2d.color = UiColors.background
        g2d.fillRect(0, 0, width, height)
        maxCharBounds = g2d.fontMetrics.getMaxCharBounds(g2d)

        // Draw the chart
        g2d.drawImage(logLevelChart.display(width, chartHeight, x, y), x, y, width, chartHeight, null)

        g2d.color = UiColors.magenta
        paintLineItem()
        return image
    }


    override fun repaint(componentOwn: ComponentOwn) {
        g2d.drawImage(
            componentOwn.display(componentOwn.width, componentOwn.height, componentOwn.x, componentOwn.y),
            componentOwn.x,
            componentOwn.y,
            componentOwn.width,
            componentOwn.height,
            null
        )
        panel.repaint(componentOwn.x, componentOwn.y, componentOwn.width, componentOwn.height)
    }

    private fun paintLineItem() {
        lineList.forEach {
            g2d.drawImage(it.display(it.width, it.height, it.x, it.y), it.x, it.y, it.width, it.height, null)
        }
    }


    override fun keyTyped(e: KeyEvent) {

    }

    override fun keyPressed(e: KeyEvent) {
        when (e.keyCode) {
            KeyEvent.VK_PAGE_UP -> {
                indexOffset += (height) / maxCharBounds.height.toInt()
                indexOffset = ensureIndexOffset(indexOffset)
                updateResults()
                setFollow()
            }

            KeyEvent.VK_UP -> {
                indexOffset += 1
                indexOffset = ensureIndexOffset(indexOffset)
                updateResults()
                setFollow()
            }

            KeyEvent.VK_PAGE_DOWN -> {
                indexOffset -= (height) / maxCharBounds.height.toInt()
                indexOffset = ensureIndexOffset(indexOffset)
                updateResults()
                setFollow()
            }

            KeyEvent.VK_DOWN -> {
                indexOffset -= 1
                indexOffset = ensureIndexOffset(indexOffset)
                updateResults()
                setFollow()
            }

            KeyEvent.VK_ENTER -> {
                indexOffset = 0
                State.lock.set(0)
                setFollow()
                updateResults()
            }

            KeyEvent.VK_C -> {

            }

            else -> {

            }
        }
        panel.repaint()
    }

    override fun keyReleased(e: KeyEvent) {

    }

    override fun mouseClicked(e: MouseEvent) {
        // Check if mouse is over the chart
        if (e.y < chartHeight) {
            logLevelChart.mouseClicked(e)
        }
        // Otherwise, handle clicks on log lines
        else if (e.clickCount == 1) {
            lineList.firstOrNull { mouseInside(e, it) }?.mouseClicked(e)
            updateResults()
        }
    }

    override fun mousePressed(e: MouseEvent) {
        // Check if mouse is over the chart
        if (e.y < chartHeight) {
            logLevelChart.mousePressed(e)
        }
        // Otherwise, handle as before
        else {
            mouseposX = e.x - x
            mouseposY = e.y - y - 7
            mouseposXPressed = mouseposX
            mouseposYPressed = mouseposY
            mouseposXReleased = mouseposX
            mouseposYReleased = mouseposY
            selectedTextRange = null
        }

        panel.repaint()
    }

    override fun mouseReleased(e: MouseEvent) {
        // Check if mouse is over the chart
        if (e.y < chartHeight) {
            logLevelChart.mouseReleased(e)
        }
        // No other action needed
    }


    override fun mouseEntered(e: MouseEvent) {
        if (!mouseInside) {
            mouseInside = true
        }

        // Check if mouse is over the chart
        if (e.y < chartHeight) {
            logLevelChart.mouseEntered(e)
        }

        // Otherwise, it's over the log lines
        else {
            lineList.firstOrNull { mouseInside(e, it) }?.mouseEntered(e)
        }
    }

    override fun mouseExited(e: MouseEvent) {
        if (mouseInside) {
            mouseInside = false
        }

        // Check if mouse was over the chart
        if (e.y < chartHeight) {
            logLevelChart.mouseExited(e)
        }

    }

    override fun mouseWheelMoved(e: MouseWheelEvent) {
        // Check if mouse is over the chart
        if (e.y < chartHeight) {
            logLevelChart.mouseWheelMoved(e)
            return
        }

        // Otherwise, handle as before
        if ((e.isControlDown && !State.onMac) || (e.isMetaDown && State.onMac)) {
            rowHeight -= e.wheelRotation
            rowHeight = rowHeight.coerceIn(4..100)
            selectedLineIndex = 0
            mouseposXPressed = 0
            mouseposYPressed = 0
            mouseposXReleased = 0
            mouseposYReleased = 0
            highlightWordMouseOver = null
            setFollow()
            updateResults()
            panel.repaint()
            return
        }

        if (e.isShiftDown) {
            State.lock.getAndAdd(-((e.wheelRotation * e.scrollAmount) * 1024).toLong())
            State.lock.set(State.lock.get().coerceIn(0..Long.MAX_VALUE))
            indexOffset = ensureIndexOffset(indexOffset)

            updateResults()
            setFollow()
            panel.repaint()
            return
        }

        indexOffset -= e.wheelRotation * e.scrollAmount
        indexOffset = ensureIndexOffset(indexOffset)
        highlightWordMouseOver = null
        updateResults()
        setFollow()
        panel.repaint()
    }

    private fun setFollow() {
        follow = indexOffset == 0
    }

    private fun ensureIndexOffset(i: Int): Int {
        return i.coerceIn(
            0..Int.MAX_VALUE
        )
    }

    override fun mouseDragged(e: MouseEvent) {
        // Check if mouse is over the chart
        if (e.y < chartHeight) {
            logLevelChart.mouseDragged(e)
        }
        // Otherwise, handle as before
        else {
            mouseposX = e.x - x
            mouseposY = e.y - y - 7
        }
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y

        // Check if mouse is over the chart
        if (e.y < chartHeight) {
            logLevelChart.mouseMoved(e)
        }

        // Otherwise, it's over the log lines
        else {
            lineList.firstOrNull { mouseInside(e, it) }?.mouseMoved(e)
            lineList.firstOrNull { e.x in it.x..it.width && e.y in it.y..it.height }?.mouseEntered(e)
            lineList.filter { e.x !in it.x..it.width || e.y !in it.y..it.height }.forEach { it.mouseExited(e) }
        }
    }

    private fun updateResults() {
        val length = if (!::maxCharBounds.isInitialized || maxCharBounds.height.toInt() == 0) {
            0
        } else {
            // Adjust for chart height
            ((height - chartHeight - maxCharBounds.height.toInt()) / maxCharBounds.height.toInt()) + 1
        }
        State.offset.set(indexOffset)
        State.length.set(length)


        // Include selected levels in the search query
        Channels.searchChannel.trySendBlocking(
            QueryChanged(
                query = inputTextLine.text,
                length = length,
                offset = indexOffset,
            )
        )
    }

    private fun updateResults(result: List<DomainLine>) {
        val length = if (!::maxCharBounds.isInitialized || maxCharBounds.height.toInt() == 0) {
            0
        } else {
            // Adjust for chart height
            ((height - chartHeight - maxCharBounds.height.toInt()) / maxCharBounds.height.toInt()) + 1
        }
        if (lineList.size - 1 != length && ::maxCharBounds.isInitialized) {
            lineList = (0..length).map {
                LineItem(
                    parent = this,
                    inputTextLine = inputTextLine,
                    x = x,
                    // Adjust y position to start below the chart
                    y = chartHeight + ((maxCharBounds.height.toInt()) * (it)),
                    width = width,
                    height = ((maxCharBounds.height.toInt()))
                )
            }
        }

        EventQueue.invokeLater {
            lineList.forEach { it.setText("") }
            result.forEachIndexed { i, it ->
                lineList[i.coerceIn(lineList.indices)].setLogJson(it)
            }

            lastUpdate = System.nanoTime()
        }
    }

    private fun mouseInside(e: MouseEvent, it: ComponentOwn) = e.x in it.x..it.width && e.y in it.y..(it.y + it.height)
    
    private fun handleBarClicked(clickedTimeIndex: Int) {
        // When a bar is clicked, we receive the time index from the chart
        // We need to calculate the absolute offset by counting events to the right
        // of the clicked bar from the beginning of the chart data
        
        // Get the chart's time points to count events
        val chartTimePoints = logLevelChart.getTimePoints()
        
        if (chartTimePoints.isEmpty() || clickedTimeIndex >= chartTimePoints.size) {
            return
        }
        
        // Count total events from the clicked time point to the end (right side)
        // This gives us the absolute offset from the beginning of the data
        var eventsToRight = 0
        for (i in clickedTimeIndex until chartTimePoints.size) {
            eventsToRight += chartTimePoints[i].getTotal()
        }
        
        // Set the absolute offset (not adding to current offset)
        indexOffset = eventsToRight
        
        // Disable follow mode when user clicks on a bar
        follow = false
        
        // Update the results to reflect the new offset
        updateResults()
        panel.repaint()
    }
}

fun loadFontFromResources(size: Float = 14f): Font {
    val stream = ClassLoader.getSystemResourceAsStream("JetBrainsMono-Regular.ttf")
        ?: error("Font not found at JetBrainsMono-Regular.ttf")
    val baseFont = Font.createFont(Font.TRUETYPE_FONT, stream)
    return baseFont.deriveFont(size)
}
