package widgets

import ComponentOwn
import SlidePanel
import State
import app.*
import app.Channels.kafkaChannel
import kafka.Kafka
import kafka.Kafka.LagInfo
import util.UiColors
import util.Styles
import java.awt.Font
import java.awt.Graphics2D
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference
import javax.swing.SwingUtilities
import kotlinx.coroutines.*

class KafkaLagView(
    private val panel: SlidePanel,
    x: Int, y: Int, width: Int, height: Int
) : ComponentOwn(), KeyListener, MouseListener, MouseWheelListener, MouseMotionListener {

    private val lagInfo = AtomicReference<List<LagInfo>>(emptyList())
    private var indexOffset = 0
    private var visibleLines = 0
    private var hideTopicsWithoutLag = false
    private var sortByLag = false
    private val hideButtonRect = java.awt.Rectangle(0, 0, 200, 20)
    private val sortButtonRect = java.awt.Rectangle(0, 0, 200, 20)
    private val refreshButtonRect = java.awt.Rectangle(0, 0, 200, 20)
    private var selectedLineIndex = 0
    private var rowHeight = 12
    private lateinit var image: BufferedImage
    private lateinit var g2d: Graphics2D
    private var mouseposX = 0
    private var mouseposY = 0
    private lateinit var maxCharBounds: Rectangle2D

    init {
        this.x = x
        this.y = y
        this.height = height
        this.width = width

        // Old style: consuming lag info from kafkaCmdGuiChannel.
        // You can remove this listener if you prefer updates only via refresh.
        Thread {
            while (true) {
                try {
                    when (val msg = Channels.kafkaCmdGuiChannel.take()) {
                        is KafkaLagInfo -> {
                            lagInfo.set(msg.lagInfo)
                            SwingUtilities.invokeLater { panel.repaint() }
                        }
                        else -> {}
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }
        }.apply { isDaemon = true }.start()
    }

    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        if (this.width != width || this.height != height || this.x != x || this.y != y) {
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

        // Calculate visible lines
        visibleLines = (height / maxCharBounds.height.toInt()) - 1 // -1 for header

        // Draw the hide/show button
        hideButtonRect.x = width - 200
        hideButtonRect.y = 0
        g2d.color = UiColors.selection
        g2d.fillRect(hideButtonRect.x, hideButtonRect.y, hideButtonRect.width, hideButtonRect.height)
        g2d.color = UiColors.defaultText
        val buttonText = if (hideTopicsWithoutLag) "Show All Topics" else "Hide Topics Without Lag"
        g2d.drawString(buttonText, hideButtonRect.x + 10, hideButtonRect.y + 15)

        // Draw the sort button
        sortButtonRect.x = width - 410
        sortButtonRect.y = 0
        g2d.color = UiColors.selection
        g2d.fillRect(sortButtonRect.x, sortButtonRect.y, sortButtonRect.width, sortButtonRect.height)
        g2d.color = UiColors.defaultText
        val sortButtonText = if (sortByLag) "Unsort" else "Sort by Lag (Most to Least)"
        g2d.drawString(sortButtonText, sortButtonRect.x + 10, sortButtonRect.y + 15)

        // Draw the refresh button
        refreshButtonRect.x = width - 620
        refreshButtonRect.y = 0
        g2d.color = UiColors.selection
        g2d.fillRect(refreshButtonRect.x, refreshButtonRect.y, refreshButtonRect.width, refreshButtonRect.height)
        g2d.color = UiColors.defaultText
        val refreshButtonText = "Refresh (Cmd + R)"
        g2d.drawString(refreshButtonText, refreshButtonRect.x + 10, refreshButtonRect.y + 15)

        // Measure column header widths
        val fontMetrics = g2d.fontMetrics
        val columnHeaders = listOf("Group ID", "Topic", "Partition", "Current Offset", "End Offset", "Lag")
        var currentLagInfo = lagInfo.get()

        val columnWidths = columnHeaders.mapIndexed { index, header ->
            val maxDataWidth = currentLagInfo.maxOfOrNull {
                when (index) {
                    0 -> fontMetrics.stringWidth(it.groupId)
                    1 -> fontMetrics.stringWidth(it.topic)
                    2 -> fontMetrics.stringWidth(it.partition.toString())
                    3 -> fontMetrics.stringWidth(it.currentOffset.toString())
                    4 -> fontMetrics.stringWidth(it.endOffset.toString())
                    5 -> fontMetrics.stringWidth(it.lag.toString())
                    else -> 0
                }
            } ?: 0
            maxOf(fontMetrics.stringWidth(header), maxDataWidth) + 20 // padding
        }

        // Calculate column x-offsets
        val columnOffsets = columnWidths.runningFold(0) { acc, w -> acc + w }

        // Draw header
        g2d.color = UiColors.magenta
        for (col in columnHeaders.indices) {
            g2d.drawString(columnHeaders[col], columnOffsets[col], maxCharBounds.height.toInt())
        }

        // Filter and sort data if necessary
        if (hideTopicsWithoutLag) {
            currentLagInfo = currentLagInfo.filter { it.lag > 0 }
        }
        if (sortByLag) {
            currentLagInfo = currentLagInfo.sortedByDescending { it.lag }
        }

        // Only display the visible items based on indexOffset
        val startIndex = indexOffset.coerceAtMost(maxOf(0, currentLagInfo.size - 1))
        val endIndex = minOf(startIndex + visibleLines, currentLagInfo.size)
        for (i in startIndex until endIndex) {
            val info = currentLagInfo[i]
            g2d.color = if (info.lag > 0) UiColors.red else UiColors.green
            val values = listOf(
                info.groupId,
                info.topic,
                info.partition.toString(),
                info.currentOffset.toString(),
                info.endOffset.toString(),
                info.lag.toString()
            )
            for (col in values.indices) {
                g2d.drawString(values[col], columnOffsets[col], maxCharBounds.height.toInt() * ((i - startIndex) + 2))
            }
        }

        if (currentLagInfo.isEmpty()) {
            g2d.color = UiColors.defaultText
            g2d.drawString("No consumer lag information available. Press Cmd+R to refresh.", 0, maxCharBounds.height.toInt() * 2)
        }
    }

    private fun drawSelectedLine() {
        if (selectedLineIndex in indexOffset until (indexOffset + visibleLines)) {
            g2d.color = UiColors.selectionLine
            val selHeight = maxCharBounds.height.toInt() + g2d.fontMetrics.maxDescent
            g2d.fillRect(0, maxCharBounds.height.toInt() * (selectedLineIndex - indexOffset + 1), width, selHeight)
        }
    }

    override fun keyTyped(e: KeyEvent) {
        // Not implemented
    }

    override fun keyPressed(e: KeyEvent) {
        if (((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_R) {
            refreshLagInfo()
            panel.repaint()
            return
        }

        val currentLagInfo = lagInfo.get()
        when (e.keyCode) {
            KeyEvent.VK_DOWN -> {
                selectedLineIndex = (selectedLineIndex + 1).coerceIn(0, currentLagInfo.size - 1)
                if (selectedLineIndex >= indexOffset + visibleLines) {
                    indexOffset = selectedLineIndex - visibleLines + 1
                }
                panel.repaint()
            }
            KeyEvent.VK_UP -> {
                selectedLineIndex = (selectedLineIndex - 1).coerceAtLeast(0)
                if (selectedLineIndex < indexOffset) {
                    indexOffset = selectedLineIndex
                }
                panel.repaint()
            }
            KeyEvent.VK_PAGE_DOWN -> {
                indexOffset = (indexOffset + visibleLines).coerceIn(0, maxOf(0, currentLagInfo.size - visibleLines))
                selectedLineIndex = (selectedLineIndex + visibleLines).coerceIn(0, currentLagInfo.size - 1)
                panel.repaint()
            }
            KeyEvent.VK_PAGE_UP -> {
                indexOffset = (indexOffset - visibleLines).coerceAtLeast(0)
                selectedLineIndex = (selectedLineIndex - visibleLines).coerceAtLeast(0)
                panel.repaint()
            }
            KeyEvent.VK_HOME -> {
                indexOffset = 0
                selectedLineIndex = 0
                panel.repaint()
            }
            KeyEvent.VK_END -> {
                indexOffset = maxOf(0, currentLagInfo.size - visibleLines)
                selectedLineIndex = currentLagInfo.size - 1
                panel.repaint()
            }
        }
    }

    override fun keyReleased(e: KeyEvent) {
        panel.repaint()
    }

    override fun mouseClicked(e: MouseEvent) { }

    override fun mousePressed(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7

        // Hide/Show button
        if (e.x in hideButtonRect.x until (hideButtonRect.x + hideButtonRect.width) &&
            e.y in hideButtonRect.y until (hideButtonRect.y + hideButtonRect.height)) {
            hideTopicsWithoutLag = !hideTopicsWithoutLag
            indexOffset = 0
        }

        // Sort button
        if (e.x in sortButtonRect.x until (sortButtonRect.x + sortButtonRect.width) &&
            e.y in sortButtonRect.y until (sortButtonRect.y + sortButtonRect.height)) {
            sortByLag = !sortByLag
            indexOffset = 0
        }

        // Refresh button
        if (e.x in refreshButtonRect.x until (refreshButtonRect.x + refreshButtonRect.width) &&
            e.y in refreshButtonRect.y until (refreshButtonRect.y + refreshButtonRect.height)) {
            refreshLagInfo()
        }
        panel.repaint()
    }

    override fun mouseReleased(e: MouseEvent) { }

    override fun mouseEntered(e: MouseEvent) { mouseInside = true }

    override fun mouseExited(e: MouseEvent) { mouseInside = false }

    override fun mouseWheelMoved(e: MouseWheelEvent) {
        if ((e.isControlDown && !State.onMac) || (e.isMetaDown && State.onMac)) {
            rowHeight += e.wheelRotation
            rowHeight = rowHeight.coerceIn(1, 100)
            panel.repaint()
            return
        }
        indexOffset += e.wheelRotation * e.scrollAmount
        indexOffset = indexOffset.coerceIn(0, maxOf(0, lagInfo.get().size - visibleLines))
        panel.repaint()
    }

    override fun mouseDragged(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7
        panel.repaint()
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7
        panel.repaint()
    }

    @OptIn(DelicateCoroutinesApi::class)
    private fun refreshLagInfo() {
        // Launch a coroutine to send a ListLag message and await the result.
        GlobalScope.launch {
            val msg = ListLag() // ListLag now carries a CompletableDeferred<List<Kafka.LagInfo>>
            kafkaChannel.put(msg)
            // Wait for the result from the Kafka consumer thread.
            val result = msg.result.await()
            lagInfo.set(result)
            SwingUtilities.invokeLater { panel.repaint() }
        }
    }
}
