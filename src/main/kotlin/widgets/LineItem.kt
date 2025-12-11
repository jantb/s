@file:OptIn(ExperimentalTime::class)

package widgets

import ColoredText
import ComponentOwn
import State
import app.*
import kotlinx.coroutines.channels.trySendBlocking
import util.UiColors
import java.awt.Graphics2D
import java.awt.event.KeyEvent
import java.awt.event.MouseEvent
import java.awt.event.MouseWheelEvent
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import kotlin.math.absoluteValue
import kotlin.time.ExperimentalTime
import kotlin.time.Instant

class LineItem(
    val parent: ComponentOwn,
    val inputTextLine: InputTextLine,
    x: Int,
    y: Int,
    width: Int,
    height: Int
) : ComponentOwn() {

    private val text: ColoredText = ColoredText()
    private var image: BufferedImage
    private var g2d: Graphics2D
    private var maxCharBounds: Rectangle2D
    private var mouseposX = 0
    private var mouseposY = 0
    private var domain: DomainLine? = null

    // Pre-calculate delimiters for faster lookup
    private val wordDelimiters = charArrayOf(' ', '"', '\'', ':', ',', '{', '}', '[', ']', '(', ')')

    init {
        this.x = x
        this.y = y
        this.height = height
        this.width = width

        // Initial buffer creation
        this.image = createBuffer(width, height)
        this.g2d = createGraphics(this.image, height)
        this.maxCharBounds = g2d.fontMetrics.getMaxCharBounds(g2d)
    }

    private fun createBuffer(w: Int, h: Int): BufferedImage {
        return BufferedImage(
            w.coerceAtLeast(1),
            h.coerceAtLeast(1),
            BufferedImage.TYPE_INT_RGB
        )
    }

    private fun createGraphics(img: BufferedImage, h: Int): Graphics2D {
        return img.createGraphics().apply {
            font = loadFontFromResources((h / 1.2).toInt().coerceIn(1..100).toFloat())
        }
    }

    fun setText(text: String) {
        g2d.color = UiColors.background
        g2d.fillRect(0, 0, width, height)

        this.text.clear()
        this.text.addText(text, color = UiColors.defaultText)

        updateHighlighting()
    }

    fun setLogJson(domainLine: DomainLine) {
        this.domain = domainLine
        this.text.clear()

        // Common timestamp
        this.text.addText(
            Instant.fromEpochMilliseconds(domainLine.timestamp).toString(),
            color = UiColors.teal
        )
        this.text.addText(" ", color = UiColors.defaultText)

        when (domainLine) {
            is LogLineDomain -> formatLogLine(domainLine)
            is KafkaLineDomain -> formatKafkaLine(domainLine)
        }

        updateHighlighting()
    }

    private fun formatLogLine(line: LogLineDomain) {
        with(text) {
            addText(line.level.name, color = getLevelColor(line.level))
            addText(" ", color = UiColors.defaultText)

            val serviceColor = UiColors.visibleColors[line.indexIdentifier.hashCode().absoluteValue % UiColors.visibleColors.size]
            addText(line.serviceName, color = serviceColor)

            line.correlationId?.let {
                addText(" ", color = UiColors.defaultText)
                addText(it, color = UiColors.defaultText)
            }

            addText(" ", color = UiColors.defaultText)
            addText(line.message.take(1000), color = UiColors.defaultText)

            line.errorMessage?.let {
                addText(" ", color = UiColors.defaultText)
                addText(it, color = UiColors.defaultText)
            }

            line.stacktrace?.let {
                addText(" ", color = UiColors.defaultText)
                addText(it, color = UiColors.defaultText)
            }
        }
    }

    private fun formatKafkaLine(line: KafkaLineDomain) {
        with(text) {
            addText(line.level.name, color = getLevelColor(line.level))
            addText(" ", color = UiColors.defaultText)
            addText(line.compositeEventId, color = UiColors.magenta)
            addText(" ", color = UiColors.defaultText)

            val topicColor = UiColors.visibleColors[line.indexIdentifier.hashCode().absoluteValue % UiColors.visibleColors.size]
            addText(line.topic, color = topicColor)

            line.correlationId?.let {
                addText(" ", color = UiColors.defaultText)
                addText(it, color = UiColors.defaultText)
            }
            line.requestId?.let {
                addText(" ", color = UiColors.defaultText)
                addText(it, color = UiColors.defaultText)
            }
            addText(" ", color = UiColors.defaultText)
            addText(line.message, color = UiColors.defaultText)
        }
    }

    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        // Only recreate buffer if dimensions or position changed materially
        if (this.width != width || this.height != height || this.x != x || this.y != y) {
            this.g2d.dispose() // Clean up old graphics

            this.width = width
            this.height = height.coerceIn(1..Int.MAX_VALUE)
            this.x = x
            this.y = y

            this.image = createBuffer(width.coerceAtLeast(1), this.height)
            this.g2d = createGraphics(this.image, height)
            this.maxCharBounds = g2d.fontMetrics.getMaxCharBounds(g2d)
        }

        g2d.color = when {
            domain is KafkaLineDomain -> UiColors.backgroundTeal
            mouseInside -> UiColors.selectionLine
            else -> UiColors.background
        }
        g2d.fillRect(0, 0, width, height)

        paintText()

        return image
    }

    private fun paintText() {
        if (text.isNotBlank()) {
            val drawX = 0
            // Align to baseline roughly
            val drawY = maxCharBounds.height.toInt() - g2d.fontMetrics.maxDescent

            // Only calculate highlight range if mouse is inside to save cycles
            text.highlight = mouseInside && text.highlightRange != null
            text.print(drawX, drawY, g2d, width)
        }
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y

        val isModifierDown = if (State.onMac) e.isMetaDown else e.isControlDown

        if (isModifierDown) {
            val charIndex = getCharIndexFromMouse(text.text, mouseposX)
            text.highlightRange = getWordRangeAtIndex(text.text, charIndex)
        } else {
            text.highlightRange = null
        }
        parent.repaint(this)
    }

    private fun updateHighlighting() {
        if (this.text.highlight) {
            val charIndex = getCharIndexFromMouse(this.text.text, mouseposX)
            this.text.highlightRange = getWordRangeAtIndex(this.text.text, charIndex)
        }
    }

    /**
     * Efficiently calculates character index from X coordinate.
     * Complexity: O(n)
     */
    private fun getCharIndexFromMouse(string: String, x: Int): Int {
        if (string.isEmpty()) return 0
        if (x <= 0) return 0

        val fm = g2d.fontMetrics
        var currentWidth = 0

        // Optimization: For strict monospaced fonts, one could use division (x / charWidth).
        // However, this iteration handles proportional fonts correctly.
        for ((index, char) in string.withIndex()) {
            val charWidth = fm.charWidth(char)
            // If the click is past the midpoint of the character, select the next one
            if (currentWidth + (charWidth / 2) >= x) {
                return index
            }
            currentWidth += charWidth
        }
        return string.length
    }

    /**
     * Finds the range of the word at the specific character index by scanning
     * left and right for delimiters.
     * Complexity: O(length of word) - Zero allocation
     */
    private fun getWordRangeAtIndex(input: String, charIndex: Int): IntRange? {
        if (input.isEmpty() || charIndex < 0 || charIndex > input.length) return null

        // Handle edge case where mouse is at the very end
        val safeIndex = if (charIndex == input.length) charIndex - 1 else charIndex

        // If we clicked on a delimiter, return null or a single char range
        if (isDelimiter(input[safeIndex])) return null

        var start = safeIndex
        var end = safeIndex

        // Scan left
        while (start > 0 && !isDelimiter(input[start - 1])) {
            start--
        }

        // Scan right
        while (end < input.lastIndex && !isDelimiter(input[end + 1])) {
            end++
        }

        return IntRange(start, end)
    }

    private fun isDelimiter(c: Char): Boolean {
        return c in wordDelimiters
    }

    override fun mouseClicked(e: MouseEvent) {
        val isModifierDown = if (State.onMac) e.isMetaDown else e.isControlDown

        if (e.isShiftDown && !e.isControlDown && e.clickCount == 1) {
            domain?.let { domainLine ->
                ModernTextViewerWindow("Log Details", domainLine).apply {
                    isVisible = true
                }
            }
        } else if (isModifierDown) {
            val highlighted = text.getHighlightedText()
            if (highlighted.isNotEmpty()) {
                inputTextLine.text = highlighted
                inputTextLine.cursorIndex = inputTextLine.text.length

                Channels.searchChannel.trySendBlocking(
                    QueryChanged(
                        highlighted,
                        length = State.length.get(),
                        offset = State.offset.get()
                    )
                )
            }
        }
    }

    override fun mouseEntered(e: MouseEvent) {
        if (!mouseInside) {
            mouseInside = true
            parent.repaint(this)
        }
    }

    override fun mouseExited(e: MouseEvent) {
        if (mouseInside) {
            mouseInside = false
            parent.repaint(this)
        }
    }

    // Boilerplate for unimplemented methods
    override fun repaint(componentOwn: ComponentOwn) { TODO("Not yet implemented") }
    override fun keyTyped(e: KeyEvent?) { TODO("Not yet implemented") }
    override fun keyPressed(e: KeyEvent?) { TODO("Not yet implemented") }
    override fun keyReleased(e: KeyEvent?) { TODO("Not yet implemented") }
    override fun mousePressed(e: MouseEvent?) { TODO("Not yet implemented") }
    override fun mouseReleased(e: MouseEvent?) { TODO("Not yet implemented") }
    override fun mouseWheelMoved(e: MouseWheelEvent?) { TODO("Not yet implemented") }
    override fun mouseDragged(e: MouseEvent?) { TODO("Not yet implemented") }
}