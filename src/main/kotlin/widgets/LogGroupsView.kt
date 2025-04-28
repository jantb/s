package widgets

import ComponentOwn
import LogCluster
import LogLevel
import SlidePanel
import State
import app.Channels.cmdGuiChannel
import app.Channels.refreshChannel
import app.LogClusterList
import app.RefreshLogGroups
import util.Styles
import util.UiColors
import java.awt.Font
import java.awt.Graphics2D
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import java.util.concurrent.atomic.AtomicReference
import javax.swing.SwingUtilities

class LogGroupsView(private val panel: SlidePanel, x: Int, y: Int, width: Int, height: Int) : ComponentOwn(),
    KeyListener,
    MouseListener,
    MouseWheelListener,
    MouseMotionListener {

    private val logClusters = AtomicReference<List<LogCluster>>(emptyList())
    private var indexOffset = 0
    private var visibleLines = 0
    private var hideLowSeverity = false
    private var sortByCount = false
    private val hideButtonRect = java.awt.Rectangle(0, 0, 200, 20)
    private val sortButtonRect = java.awt.Rectangle(0, 0, 200, 20)
    private val refreshButtonRect = java.awt.Rectangle(0, 0, 200, 20)

    init {
        this.x = x
        this.y = y
        this.width = width
        this.height = height

        val thread = Thread {
            while (true) {
                try {
                    when (val msg = cmdGuiChannel.take()) {
                        is LogClusterList -> {
                            logClusters.set(msg.clusters)
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
                g2d.dispose()
            }
            this.image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
            this.g2d = image.createGraphics()
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

        // Draw buttons
        // Hide/Show low severity button
        hideButtonRect.x = width - 200
        hideButtonRect.y = 0
        g2d.color = UiColors.selection
        g2d.fillRect(hideButtonRect.x, hideButtonRect.y, hideButtonRect.width, hideButtonRect.height)
        g2d.color = UiColors.defaultText
        val hideButtonText = if (hideLowSeverity) "Show All Levels" else "Hide Low Level"
        g2d.drawString(hideButtonText, hideButtonRect.x + 10, hideButtonRect.y + 15)

        // Sort button
        sortButtonRect.x = width - 410
        sortButtonRect.y = 0
        g2d.color = UiColors.selection
        g2d.fillRect(sortButtonRect.x, sortButtonRect.y, sortButtonRect.width, sortButtonRect.height)
        g2d.color = UiColors.defaultText
        val sortButtonText = if (sortByCount) "Unsort" else "Sort by Count"
        g2d.drawString(sortButtonText, sortButtonRect.x + 10, sortButtonRect.y + 15)

        // Refresh button
        refreshButtonRect.x = width - 620
        refreshButtonRect.y = 0
        g2d.color = UiColors.selection
        g2d.fillRect(refreshButtonRect.x, refreshButtonRect.y, refreshButtonRect.width, refreshButtonRect.height)
        g2d.color = UiColors.defaultText
        g2d.drawString("Refresh", refreshButtonRect.x + 10, refreshButtonRect.y + 15)

        // Measure column header widths
        val fontMetrics = g2d.fontMetrics
        val columnHeaders = listOf("Count", "Message Pattern", "Level")
        val sampleData = logClusters.get()

        val columnWidths = columnHeaders.mapIndexed { index, header ->
            val maxDataWidth = sampleData.maxOfOrNull {
                when (index) {
                    0 -> fontMetrics.stringWidth(it.count.toString())
                    1 -> fontMetrics.stringWidth(it.block)
                    2 -> fontMetrics.stringWidth(it.level.name)
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

        // Draw log clusters
        var currentClusters = logClusters.get()

        // Filter out low severity logs (DEBUG and INFO) if flag is set
        if (hideLowSeverity) {
            currentClusters = currentClusters.filter {
                it.level != LogLevel.DEBUG && it.level != LogLevel.INFO
            }
        }

        // Sort by count if flag is set
        if (sortByCount) {
            currentClusters = currentClusters.sortedByDescending { it.count }
        }

        // Display visible items based on indexOffset
        val startIndex = indexOffset.coerceAtMost(maxOf(0, currentClusters.size - 1))
        val endIndex = minOf(startIndex + visibleLines, currentClusters.size)

        for (i in startIndex until endIndex) {
            val cluster = currentClusters[i]
            val lineColor = when (cluster.level) {
                LogLevel.ERROR -> UiColors.red
                LogLevel.WARN -> UiColors.orange
                else -> UiColors.green
            }
            g2d.color = lineColor

            val values = listOf(
                cluster.count.toString(),
                cluster.block,
                cluster.level.name
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

        // Show message if no clusters
        if (currentClusters.isEmpty()) {
            g2d.color = UiColors.defaultText
            g2d.drawString("No log clusters available. Press Cmd+R to refresh.", 0, maxCharBounds.height.toInt() * 2)
        }
    }

    private fun drawSelectedLine() {
        if (selectedLineIndex >= indexOffset && selectedLineIndex < indexOffset + visibleLines) {
            g2d.color = UiColors.selectionLine
            val lineHeight = maxCharBounds.height.toInt() + g2d.fontMetrics.maxDescent
            g2d.fillRect(0, maxCharBounds.height.toInt() * (selectedLineIndex - indexOffset + 1), width, lineHeight)
        }
    }

    override fun keyPressed(e: KeyEvent) {
        if (((e.isMetaDown && State.onMac) || (e.isControlDown && !State.onMac)) && e.keyCode == KeyEvent.VK_R) {
            refreshChannel.trySend(RefreshLogGroups)
            panel.repaint()
        } else {
            val currentClusters = logClusters.get()

            when (e.keyCode) {
                KeyEvent.VK_DOWN -> {
                    selectedLineIndex++
                    selectedLineIndex = selectedLineIndex.coerceIn(0 until currentClusters.size)
                    if (selectedLineIndex >= indexOffset + visibleLines) {
                        indexOffset = selectedLineIndex - visibleLines + 1
                    }
                    panel.repaint()
                }

                KeyEvent.VK_UP -> {
                    selectedLineIndex--
                    selectedLineIndex = selectedLineIndex.coerceAtLeast(0)
                    if (selectedLineIndex < indexOffset) {
                        indexOffset = selectedLineIndex
                    }
                    panel.repaint()
                }

                KeyEvent.VK_PAGE_DOWN -> {
                    indexOffset += visibleLines
                    indexOffset = indexOffset.coerceIn(0, maxOf(0, currentClusters.size - visibleLines))
                    selectedLineIndex += visibleLines
                    selectedLineIndex = selectedLineIndex.coerceIn(0, currentClusters.size - 1)
                    panel.repaint()
                }

                KeyEvent.VK_PAGE_UP -> {
                    indexOffset -= visibleLines
                    indexOffset = indexOffset.coerceAtLeast(0)
                    selectedLineIndex -= visibleLines
                    selectedLineIndex = selectedLineIndex.coerceAtLeast(0)
                    panel.repaint()
                }

                KeyEvent.VK_HOME -> {
                    indexOffset = 0
                    selectedLineIndex = 0
                    panel.repaint()
                }

                KeyEvent.VK_END -> {
                    indexOffset = maxOf(0, currentClusters.size - visibleLines)
                    selectedLineIndex = currentClusters.size - 1
                    panel.repaint()
                }

                KeyEvent.VK_R -> {
                    refreshChannel.trySend(RefreshLogGroups)
                    panel.repaint()
                }
            }
        }
    }

    override fun keyTyped(e: KeyEvent) {}
    override fun keyReleased(e: KeyEvent) {
        panel.repaint()
    }

    override fun mousePressed(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7

        // Check button clicks
        if (e.x >= hideButtonRect.x && e.x <= hideButtonRect.x + hideButtonRect.width &&
            e.y >= hideButtonRect.y && e.y <= hideButtonRect.y + hideButtonRect.height
        ) {
            hideLowSeverity = !hideLowSeverity
            indexOffset = 0
        }

        if (e.x >= sortButtonRect.x && e.x <= sortButtonRect.x + sortButtonRect.width &&
            e.y >= sortButtonRect.y && e.y <= sortButtonRect.y + sortButtonRect.height
        ) {
            sortByCount = !sortByCount
            indexOffset = 0
        }

        if (e.x >= refreshButtonRect.x && e.x <= refreshButtonRect.x + refreshButtonRect.width &&
            e.y >= refreshButtonRect.y && e.y <= refreshButtonRect.y + refreshButtonRect.height
        ) {
            refreshChannel.trySend(RefreshLogGroups)
        }
        panel.repaint()
    }

    override fun mouseClicked(e: MouseEvent) {}
    override fun mouseReleased(e: MouseEvent) {}
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

        indexOffset += e.wheelRotation * e.scrollAmount
        val currentClusters = logClusters.get()
        indexOffset = indexOffset.coerceIn(0, maxOf(0, currentClusters.size - visibleLines))
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