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
import java.awt.*
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.geom.RoundRectangle2D
import java.awt.image.BufferedImage
import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference
import javax.swing.SwingUtilities
import kotlinx.coroutines.*
import kotlin.math.max
import kotlin.math.min

class KafkaLagView(
    private val panel: SlidePanel,
    x: Int, y: Int, width: Int, height: Int
) : ComponentOwn(), KeyListener, MouseListener, MouseWheelListener, MouseMotionListener {

    private val lagInfo = AtomicReference<List<LagInfo>>(emptyList())
    private var indexOffset = 0
    private var visibleLines = 0
    private var hideTopicsWithoutLag = false
    private var sortByLag = false
    private val hideButtonRect = Rectangle(0, 0, 200, 25)
    private val sortButtonRect = Rectangle(0, 0, 200, 25)
    private val refreshButtonRect = Rectangle(0, 0, 200, 25)
    
    // Chart properties
    private var chartHeight = 140
    private var hoveredBar: Pair<Int, String>? = null // Using topic name as key for hover
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
        val topic: String,
        val lag: Long,
        val percentage: Float
    )

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
            this.g2d = this.image.createGraphics().apply {
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

        // Draw header with enhanced styling
        g2d.font = headerBoldFont
        g2d.color = UiColors.magenta
        for (col in columnHeaders.indices) {
            g2d.drawString(columnHeaders[col], columnOffsets[col], chartHeight + maxCharBounds.height.toInt() + 10)
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
        
        // Draw data rows with enhanced styling
        g2d.font = Font(Styles.normalFont, Font.PLAIN, rowHeight)
        for (i in startIndex until endIndex) {
            val info = currentLagInfo[i]
            
            // Draw each value in its column with different colors
            val values = listOf(
                info.groupId,
                info.topic,
                info.partition.toString(),
                info.currentOffset.toString(),
                info.endOffset.toString(),
                info.lag.toString()
            )
            
            for (col in values.indices) {
                // Set color based on column
                g2d.color = when (col) {
                    0 -> UiColors.magenta // Group ID column
                    1 -> UiColors.teal // Topic column
                    2 -> UiColors.orange // Partition column
                    3 -> UiColors.green // Current Offset column
                    4 -> UiColors.defaultText // End Offset column
                    5 -> if (info.lag > 0) UiColors.red else UiColors.green // Lag column
                    else -> UiColors.defaultText
                }
                
                g2d.drawString(
                    values[col],
                    columnOffsets[col],
                    chartHeight + maxCharBounds.height.toInt() * ((i - startIndex) + 3) + 10
                )
            }
        }

        if (currentLagInfo.isEmpty()) {
            g2d.color = UiColors.defaultText
            g2d.drawString("No consumer lag information available. Press Cmd+R to refresh.", 10, chartHeight + maxCharBounds.height.toInt() * 3)
        }
        
        // Draw tooltip if needed
        drawTooltip()
    }
    
    private fun drawChart() {
        val lagData = lagInfo.get()
        if (lagData.isEmpty()) {
            drawEmptyChart()
            return
        }
        
        // Calculate topic lags (aggregate by topic)
        val topicLags = mutableMapOf<String, Long>()
        var totalLag = 0L
        
        lagData.forEach { info ->
            if (!hideTopicsWithoutLag || info.lag > 0) {
                topicLags[info.topic] = topicLags.getOrDefault(info.topic, 0L) + info.lag
                totalLag += info.lag
            }
        }
        
        // Draw chart background
        g2d.color = Color(UiColors.background.red, UiColors.background.green, UiColors.background.blue, 200)
        g2d.fillRect(0, 0, width, chartHeight)
        
        // Draw chart title
        g2d.font = headerBoldFont
        g2d.color = UiColors.defaultText.brighter()
        g2d.drawString("Kafka Consumer Lag by Topic", 10, 20)
        
        // Draw total lag
        g2d.font = headerPlainFont
        g2d.color = UiColors.teal
        g2d.drawString("Total Lag: $totalLag", 10, 35)
        
        // Draw chart bars
        val barHeight = 40
        val barY = 50
        val barSpacing = 15
        val maxBarWidth = width - 120  // Increased padding to prevent cut off
        val maxValue = topicLags.values.maxOrNull() ?: 1
        
        var x = 40  // Increased padding
        topicLags.entries.sortedByDescending { it.value }.forEach { entry ->
            val topic = entry.key
            val lag = entry.value
            if (lag > 0) {
                val barWidth = ((lag.toDouble() / maxValue) * maxBarWidth).toInt().coerceAtLeast(2)
                val barColor = UiColors.red // Red color for lag
                
                // Check if this bar is hovered
                val isHovered = hoveredBar?.second == topic
                
                // Draw glow effect for hovered bar
                if (isHovered) {
                    g2d.color = Color(barColor.red, barColor.green, barColor.blue, 100)
                    g2d.fillRoundRect(x - 2, barY - 2, barWidth + 4, barHeight + 4, 6, 6)
                }
                
                // Draw main bar with gradient
                val gradient = GradientPaint(
                    x.toFloat(), barY.toFloat(), barColor.brighter(),
                    x.toFloat(), (barY + barHeight).toFloat(), barColor.darker()
                )
                g2d.paint = gradient
                g2d.fillRoundRect(x, barY, barWidth, barHeight, 4, 4)
                
                // Draw bar border
                g2d.color = barColor.darker()
                g2d.drawRoundRect(x, barY, barWidth, barHeight, 4, 4)
                
                // Draw topic label (truncated if too long)
                g2d.font = buttonFont
                g2d.color = UiColors.defaultText
                val topicLabel = if (topic.length > 20) topic.take(17) + "..." else topic
                g2d.drawString(topicLabel, x, barY + barHeight + 15)
                
                // Draw lag and percentage
                val percentage = if (totalLag > 0) (lag.toDouble() / totalLag * 100).toFloat() else 0f
                g2d.color = UiColors.defaultText.darker()
                g2d.drawString("$lag (${String.format("%.1f", percentage)}%)", x, barY + barHeight + 30)
                
                x += barWidth + barSpacing
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
        val message = "No Kafka consumer lag data available"
        val metrics = g2d.fontMetrics
        val messageWidth = metrics.stringWidth(message)
        g2d.drawString(message, (width - messageWidth) / 2, chartHeight / 2)
        
        g2d.color = UiColors.defaultText.darker()
        g2d.drawRect(0, 0, width - 1, chartHeight - 1)
    }
    
    private fun drawButtons() {
        // Hide/Show button with enhanced styling
        hideButtonRect.x = width - 210
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
        val hideButtonText = if (hideTopicsWithoutLag) "Show All Topics" else "Hide Topics Without Lag"
        val hideButtonMetrics = g2d.fontMetrics
        val hideButtonX = hideButtonRect.x + (hideButtonRect.width - hideButtonMetrics.stringWidth(hideButtonText)) / 2
        val hideButtonY = hideButtonRect.y + (hideButtonRect.height + hideButtonMetrics.ascent) / 2 - 2
        g2d.drawString(hideButtonText, hideButtonX, hideButtonY)

        // Sort button with enhanced styling
        sortButtonRect.x = width - 420
        sortButtonRect.y = chartHeight + 5
        
        // Button background with gradient
        val sortButtonGradient = GradientPaint(
            sortButtonRect.x.toFloat(), sortButtonRect.y.toFloat(), UiColors.selection.brighter(),
            sortButtonRect.x.toFloat(), (sortButtonRect.y + sortButtonRect.height).toFloat(), UiColors.selection.darker()
        )
        g2d.paint = sortButtonGradient
        g2d.fillRoundRect(sortButtonRect.x, sortButtonRect.y, sortButtonRect.width, sortButtonRect.height, 6, 6)
        
        // Button border
        g2d.color = UiColors.defaultText.darker()
        g2d.drawRoundRect(sortButtonRect.x, sortButtonRect.y, sortButtonRect.width, sortButtonRect.height, 6, 6)
        
        // Button text
        g2d.font = buttonFont
        g2d.color = UiColors.defaultText
        val sortButtonText = if (sortByLag) "Unsort" else "Sort by Lag (Most to Least)"
        val sortButtonMetrics = g2d.fontMetrics
        val sortButtonX = sortButtonRect.x + (sortButtonRect.width - sortButtonMetrics.stringWidth(sortButtonText)) / 2
        val sortButtonY = sortButtonRect.y + (sortButtonRect.height + sortButtonMetrics.ascent) / 2 - 2
        g2d.drawString(sortButtonText, sortButtonX, sortButtonY)

        // Refresh button with enhanced styling (kept as requested)
        refreshButtonRect.x = width - 630
        refreshButtonRect.y = chartHeight + 5
        
        // Button background with gradient
        val refreshButtonGradient = GradientPaint(
            refreshButtonRect.x.toFloat(), refreshButtonRect.y.toFloat(), UiColors.selection.brighter(),
            refreshButtonRect.x.toFloat(), (refreshButtonRect.y + refreshButtonRect.height).toFloat(), UiColors.selection.darker()
        )
        g2d.paint = refreshButtonGradient
        g2d.fillRoundRect(refreshButtonRect.x, refreshButtonRect.y, refreshButtonRect.width, refreshButtonRect.height, 6, 6)
        
        // Button border
        g2d.color = UiColors.defaultText.darker()
        g2d.drawRoundRect(refreshButtonRect.x, refreshButtonRect.y, refreshButtonRect.width, refreshButtonRect.height, 6, 6)
        
        // Button text
        g2d.font = buttonFont
        g2d.color = UiColors.defaultText
        val refreshButtonText = "Refresh (Cmd + R)"
        val refreshButtonMetrics = g2d.fontMetrics
        val refreshButtonX = refreshButtonRect.x + (refreshButtonRect.width - refreshButtonMetrics.stringWidth(refreshButtonText)) / 2
        val refreshButtonY = refreshButtonRect.y + (refreshButtonRect.height + refreshButtonMetrics.ascent) / 2 - 2
        g2d.drawString(refreshButtonText, refreshButtonX, refreshButtonY)
    }

    private fun drawSelectedLine() {
        if (selectedLineIndex in indexOffset until (indexOffset + visibleLines)) {
            g2d.color = UiColors.selectionLine
            val selHeight = maxCharBounds.height.toInt() + g2d.fontMetrics.maxDescent
            g2d.fillRect(
                0, 
                chartHeight + maxCharBounds.height.toInt() * (selectedLineIndex - indexOffset + 3) + 10 - maxCharBounds.height.toInt(), 
                width, 
                selHeight
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
            
            g2d.color = UiColors.red
            g2d.drawString("${tooltip.topic}: ${tooltip.lag}", tooltipX + padding, tooltipY + padding + 15)
            
            g2d.color = UiColors.defaultText.darker()
            g2d.font = tooltipPlainFont
            g2d.drawString("Percentage: ${String.format("%.1f", tooltip.percentage)}%", tooltipX + padding, tooltipY + padding + 35)
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
        lastMouseX = e.x
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

        // Refresh button (kept as requested)
        if (e.x in refreshButtonRect.x until (refreshButtonRect.x + refreshButtonRect.width) &&
            e.y in refreshButtonRect.y until (refreshButtonRect.y + refreshButtonRect.height)) {
            refreshLagInfo()
        }
        panel.repaint()
    }

    override fun mouseReleased(e: MouseEvent) { 
        lastMouseX = e.x
        lastMouseY = e.y
    }

    override fun mouseEntered(e: MouseEvent) { 
        mouseInside = true 
    }

    override fun mouseExited(e: MouseEvent) { 
        mouseInside = false
        hoveredBar = null
        tooltipInfo = null
    }

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
        val lagData = lagInfo.get()
        if (lagData.isEmpty()) return
        
        // Calculate topic lags (aggregate by topic)
        val topicLags = mutableMapOf<String, Long>()
        var totalLag = 0L
        
        lagData.forEach { info ->
            if (!hideTopicsWithoutLag || info.lag > 0) {
                topicLags[info.topic] = topicLags.getOrDefault(info.topic, 0L) + info.lag
                totalLag += info.lag
            }
        }
        
        // Check which bar is being hovered
        var x = 40  // Match the padding used in drawChart
        val barHeight = 40
        val barY = 50
        val barSpacing = 15
        val maxBarWidth = width - 80  // Match the padding used in drawChart
        val maxValue = topicLags.values.maxOrNull() ?: 1
        
        topicLags.entries.sortedByDescending { it.value }.forEach { entry ->
            val topic = entry.key
            val lag = entry.value
            if (lag > 0) {
                val barWidth = ((lag.toDouble() / maxValue) * maxBarWidth).toInt().coerceAtLeast(2)
                
                // Check if mouse is over this bar
                if (mouseX >= x && mouseX <= x + barWidth && mouseY >= barY && mouseY <= barY + barHeight) {
                    val percentage = if (totalLag > 0) (lag.toDouble() / totalLag * 100).toFloat() else 0f
                    hoveredBar = 0 to topic // Using 0 as dummy index since we're not tracking specific bars
                    tooltipInfo = TooltipInfo(mouseX, mouseY, topic, lag, percentage)
                    return
                }
                
                x += barWidth + barSpacing
            }
        }
        
        // If we get here, no bar is hovered
        hoveredBar = null
        tooltipInfo = null
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
