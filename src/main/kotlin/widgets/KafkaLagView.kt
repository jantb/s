package widgets

import ComponentOwn
import SlidePanel
import State
import app.*
import app.Channels.kafkaCmdGuiChannel
import app.Channels.kafkaChannel
import kafka.Kafka
import util.UiColors
import util.Styles
import java.awt.Font
import java.awt.Graphics2D
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import java.util.concurrent.atomic.AtomicReference
import javax.swing.SwingUtilities

class KafkaLagView(private val panel: SlidePanel, x: Int, y: Int, width: Int, height: Int) : ComponentOwn(),
    KeyListener,
    MouseListener, MouseWheelListener,
    MouseMotionListener {
    private val lagInfo = AtomicReference<List<Kafka.LagInfo>>(emptyList())
    private var indexOffset = 0
    private var visibleLines = 0
    private var hideTopicsWithoutLag = false
    private var sortByLag = false
    private val hideButtonRect = java.awt.Rectangle(0, 0, 200, 20)
    private val sortButtonRect = java.awt.Rectangle(0, 0, 200, 20)
    private val refreshButtonRect = java.awt.Rectangle(0, 0, 200, 20)

    init {
        this.x = x
        this.y = y
        this.height = height
        this.width = width

        val thread = Thread {
            while (true) {
                try {
                    when (val msg = kafkaCmdGuiChannel.take()) {
                        is KafkaLagInfo -> {
                            lagInfo.set(msg.lagInfo)
                            SwingUtilities.invokeLater {
                                panel.repaint()
                            }
                        }
                        else -> {}
                    }
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }
        }
        thread.isDaemon = true
        thread.start()
    }

    private var selectedLineIndex = 0
    private var rowHeight = 12
    private lateinit var image: BufferedImage
    private lateinit var g2d: Graphics2D
    private var mouseposX = 0
    private var mouseposY = 0
    private lateinit var maxCharBounds: Rectangle2D

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
        //Clear
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
        val refreshButtonText = "Refresh"
        g2d.drawString(refreshButtonText, refreshButtonRect.x + 10, refreshButtonRect.y + 15)

        // Measure column header widths
        val fontMetrics = g2d.fontMetrics
        val columnHeaders = listOf("Group ID", "Topic", "Partition", "Current Offset", "End Offset", "Lag")
        val sampleData = lagInfo.get().take(visibleLines) // limit to visible for perf

        val columnWidths = columnHeaders.mapIndexed { index, header ->
            val maxDataWidth = sampleData.maxOfOrNull {
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

        // Draw lag info
        var currentLagInfo = lagInfo.get()

        // Filter out topics without lag if the flag is set
        if (hideTopicsWithoutLag) {
            currentLagInfo = currentLagInfo.filter { it.lag > 0 }
        }

        // Sort by lag if the flag is set
        if (sortByLag) {
            currentLagInfo = currentLagInfo.sortedByDescending { it.lag }
        }

        // Only display the visible items based on indexOffset
        val startIndex = indexOffset.coerceAtMost(maxOf(0, currentLagInfo.size - 1))
        val endIndex = minOf(startIndex + visibleLines, currentLagInfo.size)

        for (i in startIndex until endIndex) {
            val info = currentLagInfo[i]
            val lineColor = if (info.lag > 0) UiColors.red else UiColors.green
            g2d.color = lineColor

            val values = listOf(
                info.groupId,
                info.topic,
                info.partition.toString(),
                info.currentOffset.toString(),
                info.endOffset.toString(),
                info.lag.toString()
            )

            // Draw each value in its column
            for (col in values.indices) {
                g2d.drawString(
                    values[col],
                    columnOffsets[col],
                    maxCharBounds.height.toInt() * ((i - startIndex) + 2)
                )
            }
        }

        // If no lag info, show a message
        if (currentLagInfo.isEmpty()) {
            g2d.color = UiColors.defaultText
            g2d.drawString("No consumer lag information available. Press Cmd+L to refresh.", 0, maxCharBounds.height.toInt() * 2)
        }
    }


    private fun drawSelectedLine() {
        // Only draw selection if it's visible
        if (selectedLineIndex >= indexOffset && selectedLineIndex < indexOffset + visibleLines) {
            g2d.color = UiColors.selectionLine
            val height = maxCharBounds.height.toInt() + g2d.fontMetrics.maxDescent
            g2d.fillRect(0, maxCharBounds.height.toInt() * (selectedLineIndex - indexOffset + 1), width, height)
        }
    }

    override fun keyTyped(e: KeyEvent) {
        // Not implemented
    }

    override fun keyPressed(e: KeyEvent) {
        if (((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_L) {
            // Refresh lag info
            kafkaChannel.put(ListLag)
            panel.repaint()
        } else {
            val currentLagInfo = lagInfo.get()

            when (e.keyCode) {
                KeyEvent.VK_DOWN -> {
                    // Move selection down
                    selectedLineIndex++
                    val maxIndex = currentLagInfo.size
                    selectedLineIndex = selectedLineIndex.coerceIn(0 until maxIndex)

                    // Auto-scroll if selection goes out of view
                    if (selectedLineIndex >= indexOffset + visibleLines) {
                        indexOffset = selectedLineIndex - visibleLines + 1
                    }

                    panel.repaint()
                }
                KeyEvent.VK_UP -> {
                    // Move selection up
                    selectedLineIndex--
                    selectedLineIndex = selectedLineIndex.coerceAtLeast(0)

                    // Auto-scroll if selection goes out of view
                    if (selectedLineIndex < indexOffset) {
                        indexOffset = selectedLineIndex
                    }

                    panel.repaint()
                }
                KeyEvent.VK_PAGE_DOWN -> {
                    // Scroll down one page
                    indexOffset += visibleLines
                    indexOffset = indexOffset.coerceIn(0, maxOf(0, currentLagInfo.size - visibleLines))

                    // Move selection
                    selectedLineIndex += visibleLines
                    selectedLineIndex = selectedLineIndex.coerceIn(0, currentLagInfo.size - 1)

                    panel.repaint()
                }
                KeyEvent.VK_PAGE_UP -> {
                    // Scroll up one page
                    indexOffset -= visibleLines
                    indexOffset = indexOffset.coerceAtLeast(0)

                    // Move selection
                    selectedLineIndex -= visibleLines
                    selectedLineIndex = selectedLineIndex.coerceAtLeast(0)

                    panel.repaint()
                }
                KeyEvent.VK_HOME -> {
                    // Scroll to top
                    indexOffset = 0
                    selectedLineIndex = 0
                    panel.repaint()
                }
                KeyEvent.VK_END -> {
                    // Scroll to bottom
                    indexOffset = maxOf(0, currentLagInfo.size - visibleLines)
                    selectedLineIndex = currentLagInfo.size - 1
                    panel.repaint()
                }
            }
        }
    }

    override fun keyReleased(e: KeyEvent) {
        panel.repaint()
    }

    override fun mouseClicked(e: MouseEvent) {
        // Not implemented
    }

    override fun mousePressed(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7

        // Check if the hide/show button was clicked
        if (e.x >= hideButtonRect.x && e.x <= hideButtonRect.x + hideButtonRect.width &&
            e.y >= hideButtonRect.y && e.y <= hideButtonRect.y + hideButtonRect.height) {
            // Toggle the flag
            hideTopicsWithoutLag = !hideTopicsWithoutLag

            // Reset the index offset when toggling to ensure we start from the beginning
            indexOffset = 0
        }

        // Check if the sort button was clicked
        if (e.x >= sortButtonRect.x && e.x <= sortButtonRect.x + sortButtonRect.width &&
            e.y >= sortButtonRect.y && e.y <= sortButtonRect.y + sortButtonRect.height) {
            // Toggle the sort flag
            sortByLag = !sortByLag

            // Reset the index offset when toggling to ensure we start from the beginning
            indexOffset = 0
        }

        // Check if the refresh button was clicked
        if (e.x >= refreshButtonRect.x && e.x <= refreshButtonRect.x + refreshButtonRect.width &&
            e.y >= refreshButtonRect.y && e.y <= refreshButtonRect.y + refreshButtonRect.height) {
            // Toggle the sort flag
            kafkaChannel.put(ListLag)
        }

        panel.repaint()
    }

    override fun mouseReleased(e: MouseEvent) {
        // Not implemented
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

    override fun mouseWheelMoved(e: MouseWheelEvent) {
        if ((e.isControlDown && !State.onMac) || (e.isMetaDown && State.onMac)) {
            rowHeight += e.wheelRotation
            rowHeight = rowHeight.coerceIn(1..100)
            panel.repaint()
            return
        }

        // Scroll the view
        indexOffset += e.wheelRotation * e.scrollAmount

        // Ensure indexOffset stays within valid range
        val currentLagInfo = lagInfo.get()
        indexOffset = indexOffset.coerceIn(0, maxOf(0, currentLagInfo.size - visibleLines))

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
}
