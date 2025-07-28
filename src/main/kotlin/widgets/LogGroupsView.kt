package widgets

import ComponentOwn
import LogCluster
import LogLevel
import SlidePanel
import State
import app.Channels.logClusterCmdGuiChannel
import app.Channels.refreshChannel
import app.LogClusterList
import app.RefreshLogGroups
import util.Styles
import util.UiColors
import widgets.getLevelColor
import java.awt.*
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.geom.RoundRectangle2D
import java.awt.image.BufferedImage
import java.util.concurrent.atomic.AtomicReference
import javax.swing.SwingUtilities
import kotlin.math.max
import kotlin.math.min

class LogGroupsView(private val panel: SlidePanel, x: Int, y: Int, width: Int, height: Int) : ComponentOwn(),
    KeyListener,
    MouseListener,
    MouseWheelListener,
    MouseMotionListener {

    private val logClusters = AtomicReference<List<LogCluster>>(emptyList())
    private var indexOffset = 0
    private var visibleLines = 0
    private var hideLowSeverity = false
    private var sortByCount = true  // Always sort by descending count
    private val hideButtonRect = Rectangle(0, 0, 200, 25)
    
    // Chart properties
    private var chartHeight = 140
    private var hoveredBar: Pair<Int, LogLevel>? = null
    private var tooltipInfo: TooltipInfo? = null
    
    // Enhanced UI properties
    private var lastMouseX = 0
    private var lastMouseY = 0
    
    // Pre-calculated fonts
    private val headerBoldFont = Font(Styles.normalFont, Font.BOLD, 12)
    private val headerPlainFont = Font(Styles.normalFont, Font.PLAIN, 12)
    private val buttonFont = Font(Styles.normalFont, Font.PLAIN, 11)
    private val tooltipFont = Font(Styles.normalFont, Font.BOLD, 12)
    private val tooltipPlainFont = Font(Styles.normalFont, Font.PLAIN, 11)
    private val emptyChartFont = Font(Styles.normalFont, Font.ITALIC, 14)
    
    private data class TooltipInfo(
        val x: Int,
        val y: Int,
        val level: LogLevel,
        val count: Int,
        val percentage: Float
    )

    init {
        this.x = x
        this.y = y
        this.width = width
        this.height = height

        // Thread for receiving log cluster updates
        val receiveThread = Thread {
            while (true) {
                try {
                    when (val msg = logClusterCmdGuiChannel.take()) {
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
        receiveThread.isDaemon = true
        receiveThread.start()


        // Thread for auto-refreshing log clusters
        val refreshThread = Thread {
            while (true) {
                try {
                    Thread.sleep(200) // Refresh every 200 ms
                    refreshChannel.trySend(RefreshLogGroups)
                } catch (e: Exception) {
                    e.printStackTrace()
                }
            }
        }
        refreshThread.isDaemon = true
        refreshThread.start()
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
            this.g2d = image.createGraphics().apply {
                setRenderingHint(RenderingHints.KEY_ANTIALIASING, RenderingHints.VALUE_ANTIALIAS_ON)
                setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_ON)
                setRenderingHint(RenderingHints.KEY_RENDERING, RenderingHints.VALUE_RENDER_QUALITY)
            }
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

        // Calculate chart height and visible lines
        chartHeight = 140
        visibleLines = ((height - chartHeight - 30) / maxCharBounds.height.toInt()) - 2 // -2 for header and spacing, -30 for bottom padding

        // Draw chart
        drawChart()
        
        // Draw buttons with enhanced styling
        drawButtons()

        // Measure column header widths
        val fontMetrics = g2d.fontMetrics
        val columnHeaders = listOf("Count", "Level", "IndexId", "Message Pattern")

        val sampleData = logClusters.get()

        val columnWidths = columnHeaders.mapIndexed { index, header ->
            val maxDataWidth = sampleData.maxOfOrNull {
                when (index) {
                    0 -> fontMetrics.stringWidth(it.count.toString())
                    1 -> fontMetrics.stringWidth(it.level.name)
                    2 -> fontMetrics.stringWidth(it.indexIdentifier)
                    3 -> fontMetrics.stringWidth(it.block)
                    else -> 0
                }
            } ?: 0
            val padding = if (index == 2) 40 else 20  // More padding for indexId column
            maxOf(fontMetrics.stringWidth(header), maxDataWidth) + padding
        }

        // Calculate column x-offsets
        val columnOffsets = columnWidths.runningFold(0) { acc, w -> acc + w }

        // Draw header with enhanced styling
        g2d.font = headerBoldFont
        g2d.color = UiColors.magenta
        for (col in columnHeaders.indices) {
            g2d.drawString(columnHeaders[col], columnOffsets[col], chartHeight + maxCharBounds.height.toInt() + 10)
        }

        // Draw log clusters
        var currentClusters = logClusters.get()

        // Filter out low severity logs (DEBUG and INFO) if flag is set
        if (hideLowSeverity) {
            currentClusters = currentClusters.filter {
                it.level != LogLevel.DEBUG && it.level != LogLevel.INFO && it.level != LogLevel.UNKNOWN
            }
        }

        // Always sort by descending count
        currentClusters = currentClusters.sortedByDescending { it.count }

        // Display visible items based on indexOffset
        val startIndex = indexOffset.coerceAtMost(maxOf(0, currentClusters.size - 1))
        val endIndex = minOf(startIndex + visibleLines, currentClusters.size)

        // Draw data rows with enhanced styling
        g2d.font = Font(Styles.normalFont, Font.PLAIN, rowHeight)
        for (i in startIndex until endIndex) {
            val cluster = currentClusters[i]
            
            val values = listOf(
                cluster.count.toString(),
                cluster.level.name,
                cluster.indexIdentifier,
                cluster.block
            )

            // Draw each value in its column with different colors
            for (col in values.indices) {
                // Set color based on column
                g2d.color = when (col) {
                    0 -> UiColors.teal // Count column
                    1 -> { // Level column - keep level colors
                        when (cluster.level) {
                            LogLevel.ERROR -> UiColors.red
                            LogLevel.WARN -> UiColors.orange
                            LogLevel.DEBUG -> UiColors.teal
                            LogLevel.INFO -> UiColors.green
                            LogLevel.KAFKA -> Color(0, 188, 212) // Material Cyan
                            else -> UiColors.defaultText
                        }
                    }
                    2 -> UiColors.magenta // IndexId column
                    3 -> UiColors.defaultText // Message Pattern column
                    else -> UiColors.defaultText
                }
                
                g2d.drawString(
                    values[col],
                    columnOffsets[col],
                    chartHeight + maxCharBounds.height.toInt() * ((i - startIndex) + 3) + 10
                )
            }
        }

        // Show message if no clusters
        if (currentClusters.isEmpty()) {
            g2d.color = UiColors.defaultText
            g2d.drawString("No log clusters available.", 10, chartHeight + maxCharBounds.height.toInt() * 3)
        }
        
        // Draw tooltip if needed
        drawTooltip()
    }
    
    private fun drawChart() {
        val clusters = logClusters.get()
        if (clusters.isEmpty()) {
            drawEmptyChart()
            return
        }
        
        // Calculate level counts
        val levelCounts = mutableMapOf<LogLevel, Long>()
        var total = 0L
        
        clusters.forEach { cluster ->
            // Filter out low severity logs if flag is set
            if (!hideLowSeverity || (cluster.level != LogLevel.DEBUG && cluster.level != LogLevel.INFO && cluster.level != LogLevel.UNKNOWN)) {
                levelCounts[cluster.level] = levelCounts.getOrDefault(cluster.level, 0L) + cluster.count
                total += cluster.count
            }
        }
        
        // Draw chart background
        g2d.color = Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200)
        g2d.fillRect(0, 0, width, chartHeight)
        
        // Draw chart title
        g2d.font = headerBoldFont
        g2d.color = UiColors.defaultText.brighter()
        g2d.drawString("Log Cluster Distribution by Level (Horizontal)", 10, 20)
        
        // Draw total count
        g2d.font = headerPlainFont
        g2d.color = UiColors.teal
        g2d.drawString("Total Clusters: $total", 10, 35)
        
        // Sort levels by severity: ERROR, WARN, INFO, DEBUG, KAFKA, UNKNOWN
        val severityOrder = listOf(LogLevel.ERROR, LogLevel.WARN, LogLevel.INFO, LogLevel.DEBUG, LogLevel.KAFKA, LogLevel.UNKNOWN)
        val sortedLevels = severityOrder.filter { levelCounts[it] != null && levelCounts[it]!! > 0 }
        
        // Draw horizontal stacked bars - thinner bars and better positioning
        val barHeight = 18  // Made thinner (was 25)
        val barSpacing = 8
        val labelSpace = 100  // More space for labels
        val maxBarWidth = width - labelSpace - 150  // Leave more space for labels and values
        val maxValue = levelCounts.values.maxOrNull() ?: 1
        val startY = 55
        val barStartX = labelSpace  // Start bars further to the right
        
        sortedLevels.forEachIndexed { index, level ->
            val count = levelCounts[level] ?: 0L
            if (count > 0) {
                val y = startY + index * (barHeight + barSpacing)
                val barWidth = ((count.toDouble() / maxValue) * maxBarWidth).toInt().coerceAtLeast(2)
                val barColor = getLevelColor(level)
                
                // Check if this bar is hovered
                val isHovered = hoveredBar?.second == level
                
                // Draw glow effect for hovered bar
                if (isHovered) {
                    g2d.color = Color(barColor.red, barColor.green, barColor.blue, 100)
                    g2d.fillRoundRect(barStartX - 2, y - 2, barWidth + 4, barHeight + 4, 6, 6)
                }
                
                // Draw main bar with gradient
                val gradient = GradientPaint(
                    barStartX.toFloat(), y.toFloat(), barColor.brighter(),
                    (barStartX + barWidth).toFloat(), y.toFloat(), barColor.darker()
                )
                g2d.paint = gradient
                g2d.fillRoundRect(barStartX, y, barWidth, barHeight, 4, 4)
                
                // Draw bar border
                g2d.color = barColor.darker()
                g2d.drawRoundRect(barStartX, y, barWidth, barHeight, 4, 4)
                
                // Draw level label on the left - better positioned
                g2d.font = buttonFont
                g2d.color = UiColors.defaultText
                val labelWidth = g2d.fontMetrics.stringWidth(level.name)
                g2d.drawString(level.name, barStartX - labelWidth - 15, y + barHeight / 2 + 4)
                
                // Draw count and percentage on the right
                val percentage = if (total > 0) (count.toDouble() / total * 100).toFloat() else 0f
                g2d.color = UiColors.defaultText.darker()
                val valueText = "$count (${String.format("%.1f", percentage)}%)"
                g2d.drawString(valueText, barStartX + barWidth + 15, y + barHeight / 2 + 4)
            }
        }
        
        // Draw chart border
        g2d.color = UiColors.defaultText.darker()
        g2d.drawRect(0, 0, width - 1, chartHeight - 1)
    }
    
    private fun drawEmptyChart() {
        g2d.color = Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200)
        g2d.fillRect(0, 0, width, chartHeight)
        
        g2d.font = emptyChartFont
        g2d.color = UiColors.defaultText.darker()
        val message = "No log cluster data available"
        val metrics = g2d.fontMetrics
        val messageWidth = metrics.stringWidth(message)
        g2d.drawString(message, (width - messageWidth) / 2, chartHeight / 2)
        
        g2d.color = UiColors.defaultText.darker()
        g2d.drawRect(0, 0, width - 1, chartHeight - 1)
    }
    
    private fun drawButtons() {
        // Hide/Show low severity button with enhanced styling
        hideButtonRect.x = width - 210  // Adjusted position since refresh button is removed
        hideButtonRect.y = chartHeight + 5
        
        // Button background with gradient
        val hideButtonGradient = GradientPaint(
            hideButtonRect.x.toFloat(), hideButtonRect.y.toFloat(), UiColors.selection.brighter(),
            hideButtonRect.x.toFloat(), (hideButtonRect.y + hideButtonRect.height).toFloat(), UiColors.selection.darker()
        )
        g2d.paint = hideButtonGradient
        g2d.fillRoundRect(hideButtonRect.x, hideButtonRect.y, hideButtonRect.width, hideButtonRect.height, 6, 6)
        
        // Button border
        g2d.color = UiColors.defaultText.darker()
        g2d.drawRoundRect(hideButtonRect.x, hideButtonRect.y, hideButtonRect.width, hideButtonRect.height, 6, 6)
        
        // Button text
        g2d.font = buttonFont
        g2d.color = UiColors.defaultText
        val hideButtonText = if (hideLowSeverity) "Show All Levels" else "Hide Low Level"
        val hideButtonMetrics = g2d.fontMetrics
        val hideButtonX = hideButtonRect.x + (hideButtonRect.width - hideButtonMetrics.stringWidth(hideButtonText)) / 2
        val hideButtonY = hideButtonRect.y + (hideButtonRect.height + hideButtonMetrics.ascent) / 2 - 2
        g2d.drawString(hideButtonText, hideButtonX, hideButtonY)

    }

    private fun drawSelectedLine() {
        if (selectedLineIndex >= indexOffset && selectedLineIndex < indexOffset + visibleLines) {
            g2d.color = UiColors.selectionLine
            val lineHeight = maxCharBounds.height.toInt() + g2d.fontMetrics.maxDescent
            g2d.fillRect(
                0, 
                chartHeight + maxCharBounds.height.toInt() * (selectedLineIndex - indexOffset + 3) + 10 - maxCharBounds.height.toInt(), 
                width, 
                lineHeight
            )
        }
    }
    
    private fun drawTooltip() {
        tooltipInfo?.let { tooltip ->
            val tooltipWidth = 180
            val tooltipHeight = 70
            val padding = 8
            
            // Position tooltip to avoid edges
            var tooltipX = tooltip.x + 10
            var tooltipY = tooltip.y - tooltipHeight - 10
            
            if (tooltipX + tooltipWidth > width) tooltipX = tooltip.x - tooltipWidth - 10
            if (tooltipY < chartHeight) tooltipY = tooltip.y + 20
            
            // Draw tooltip background with shadow
            g2d.color = Color(0, 0, 0, 100)
            g2d.fillRoundRect(tooltipX + 2, tooltipY + 2, tooltipWidth, tooltipHeight, 8, 8)
            
            g2d.color = Color(40, 42, 46)
            g2d.fillRoundRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight, 8, 8)
            
            g2d.color = UiColors.defaultText.darker()
            g2d.stroke = BasicStroke(1f)
            g2d.drawRoundRect(tooltipX, tooltipY, tooltipWidth, tooltipHeight, 8, 8)
            
            // Draw tooltip content
            g2d.font = tooltipFont
            g2d.color = UiColors.defaultText
            
            g2d.color = getLevelColor(tooltip.level)
            g2d.drawString("${tooltip.level.name}: ${tooltip.count}", tooltipX + padding, tooltipY + padding + 15)
            
            g2d.color = UiColors.defaultText.darker()
            g2d.font = tooltipPlainFont
            g2d.drawString("Percentage: ${String.format("%.1f", tooltip.percentage)}%", tooltipX + padding, tooltipY + padding + 35)
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
        lastMouseX = e.x
        lastMouseY = e.y
        mouseposX = e.x - x
        mouseposY = e.y - y - 7

        // Check button clicks
        if (e.x >= hideButtonRect.x && e.x <= hideButtonRect.x + hideButtonRect.width &&
            e.y >= hideButtonRect.y && e.y <= hideButtonRect.y + hideButtonRect.height
        ) {
            hideLowSeverity = !hideLowSeverity
            indexOffset = 0
        }

        panel.repaint()
    }

    override fun mouseClicked(e: MouseEvent) {}
    
    override fun mouseReleased(e: MouseEvent) {
        lastMouseX = e.x
        lastMouseY = e.y
    }
    
    override fun mouseEntered(e: MouseEvent) {
        if (!mouseInside) {
            mouseInside = true
        }
    }

    override fun mouseExited(e: MouseEvent) {
        if (mouseInside) {
            mouseInside = false
            hoveredBar = null
            tooltipInfo = null
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
        lastMouseX = e.x
        lastMouseY = e.y
        panel.repaint()
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7
        lastMouseX = e.x
        lastMouseY = e.y
        
        // Handle chart hover interactions
        if (e.y < chartHeight && e.y > 40) {
            handleChartHover(e.x, e.y)
        } else {
            hoveredBar = null
            tooltipInfo = null
        }
        
        panel.repaint()
    }
    
    private fun handleChartHover(mouseX: Int, mouseY: Int) {
        val clusters = logClusters.get()
        if (clusters.isEmpty()) return
        
        // Calculate level counts
        val levelCounts = mutableMapOf<LogLevel, Long>()
        var total = 0L
        
        clusters.forEach { cluster ->
            // Filter out low severity logs if flag is set
            if (!hideLowSeverity || (cluster.level != LogLevel.DEBUG && cluster.level != LogLevel.INFO && cluster.level != LogLevel.UNKNOWN)) {
                levelCounts[cluster.level] = levelCounts.getOrDefault(cluster.level, 0L) + cluster.count
                total += cluster.count
            }
        }
        
        // Sort levels by severity: ERROR, WARN, INFO, DEBUG, KAFKA, UNKNOWN
        val severityOrder = listOf(LogLevel.ERROR, LogLevel.WARN, LogLevel.INFO, LogLevel.DEBUG, LogLevel.KAFKA, LogLevel.UNKNOWN)
        val sortedLevels = severityOrder.filter { levelCounts[it] != null && levelCounts[it]!! > 0 }
        
        // Check which horizontal bar is being hovered - updated positioning
        val barHeight = 18  // Match the thinner bars
        val barSpacing = 8
        val labelSpace = 100
        val maxBarWidth = width - labelSpace - 150
        val maxValue = levelCounts.values.maxOrNull() ?: 1
        val startY = 55
        val barStartX = labelSpace
        
        sortedLevels.forEachIndexed { index, level ->
            val count = levelCounts[level] ?: 0L
            if (count > 0) {
                val y = startY + index * (barHeight + barSpacing)
                val barWidth = ((count.toDouble() / maxValue) * maxBarWidth).toInt().coerceAtLeast(2)
                
                // Check if mouse is over this horizontal bar
                if (mouseX >= barStartX && mouseX <= barStartX + barWidth && mouseY >= y && mouseY <= y + barHeight) {
                    val percentage = if (total > 0) (count.toDouble() / total * 100).toFloat() else 0f
                    hoveredBar = index to level
                    tooltipInfo = TooltipInfo(mouseX, mouseY, level, count.toInt(), percentage)
                    return
                }
            }
        }
        
        // If we get here, no bar is hovered
        hoveredBar = null
        tooltipInfo = null
    }
}