package widgets

import ColoredText
import ComponentOwn
import State
import app.Channels
import app.Domain
import app.PublishToTopic
import app.QueryChanged
import deserializeJsonToObject
import kotlinx.coroutines.channels.trySendBlocking
import serializeToJsonPP
import util.Styles
import util.UiColors
import java.awt.Font
import java.awt.Graphics2D
import java.awt.event.KeyEvent
import java.awt.event.MouseEvent
import java.awt.event.MouseWheelEvent
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import java.time.Instant
import kotlin.math.absoluteValue

class LineItem(val parent: ComponentOwn, val inputTextLine: InputTextLine, x: Int, y: Int, width: Int, height: Int) :
    ComponentOwn() {
    private var text: ColoredText = ColoredText()
    private var image: BufferedImage
    private var g2d: Graphics2D
    private var maxCharBounds: Rectangle2D
    private var mouseposX = 0
    private var mouseposY = 0
    private var domain: Domain? = null

    init {
        this.x = x
        this.y = y
        this.height = height
        this.width = width
        this.image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
        this.g2d = this.image.createGraphics()
        maxCharBounds = g2d.fontMetrics.getMaxCharBounds(g2d)
    }

    fun setText(text: String) {
        this.text.clear()
        this.text.addText(text, color = UiColors.defaultText)
        if (this.text.highlight) {
            this.text.highlightRange =
                getWordRangeAtIndex(this.text.text, getCharIndexFromMouse(this.text.text, mouseposX))
        }
    }

    fun setLogJson(domain: Domain) {
        this.domain = domain
        this.text.clear()
        this.text.addText(Instant.ofEpochMilli(domain.timestamp).toString(), color = UiColors.teal)

        this.text.addText(" ", color = UiColors.defaultText)
        if (domain.topic == "") {
            this.text.addText(
                domain.level.name, color = when (domain.level) {
                    LogLevel.INFO -> {
                        UiColors.green
                    }

                    LogLevel.WARN -> {
                        UiColors.orange
                    }

                    LogLevel.DEBUG -> {
                        UiColors.defaultText
                    }

                    LogLevel.ERROR -> {
                        UiColors.red
                    }

                    else -> {
                        UiColors.defaultText
                    }
                }
            )
        }
        this.text.addText(domain.topic, color = UiColors.green)

        this.text.addText(" ", color = UiColors.defaultText)
        this.text.addText(
            domain.application,
            color = UiColors.visibleColors[domain.indexIdentifier.hashCode()
                .toInt().absoluteValue % UiColors.visibleColors.size]
        )
        this.text.addText(domain.partition, color = UiColors.magenta)

        if (domain.correlationId != "") {
            this.text.addText(" ", color = UiColors.defaultText)
            this.text.addText(domain.correlationId, color = UiColors.orange)
        }

        if (domain.requestId != "") {
            this.text.addText(" ", color = UiColors.defaultText)
            this.text.addText(domain.requestId, color = UiColors.orange)
        }

        this.text.addText(" ", color = UiColors.defaultText)
        this.text.addText(domain.message, color = UiColors.defaultText)
        this.text.addText(domain.errorMessage, color = UiColors.defaultText)
        this.text.addText(domain.offset, color = UiColors.orange)

        this.text.addText(" ", color = UiColors.defaultText)
        this.text.addText(domain.headers, color = UiColors.magenta)


        this.text.addText(" ", color = UiColors.defaultText)
        this.text.addText(domain.stacktraceType, color = UiColors.defaultText)
        this.text.addText(domain.stacktrace, color = UiColors.defaultText)
        this.text.addText("${domain.key} ${domain.data}", color = UiColors.defaultText)

        if (this.text.highlight) {
            this.text.highlightRange =
                getWordRangeAtIndex(this.text.text, getCharIndexFromMouse(this.text.text, mouseposX))
        }
    }

    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        if (this.width != width || this.height != height || this.x != x || this.y != y) {
            this.image = BufferedImage(
                width.coerceIn(1..Int.MAX_VALUE),
                this.height.coerceIn(1..Int.MAX_VALUE),
                BufferedImage.TYPE_INT_RGB
            )
            this.g2d = this.image.createGraphics()
            this.height = height.coerceIn(1..Int.MAX_VALUE)
            this.width = width
            this.x = x
            this.y = y
        }
        maxCharBounds = g2d.fontMetrics.getMaxCharBounds(g2d)
        g2d.color = if (mouseInside) UiColors.selectionLine else UiColors.background
        g2d.fillRect(0, 0, width, height)
        g2d.font = Font(Styles.normalFont, Font.PLAIN, (height / 1.2).toInt().coerceIn(1..100))
        paintText()

        return image
    }

    override fun repaint(componentOwn: ComponentOwn) {
        TODO("Not yet implemented")
    }

    private fun paintText() {
        if (text.isNotBlank()) {
            val x = 0
            val y = maxCharBounds.height.toInt() - g2d.fontMetrics.maxDescent
            text.highlight = mouseInside && text.highlightRange != null
            text.print(x, y, g2d)
        }
    }

    override fun keyTyped(e: KeyEvent?) {
        TODO("Not yet implemented")
    }

    override fun keyPressed(e: KeyEvent?) {
        TODO("Not yet implemented")
    }

    override fun keyReleased(e: KeyEvent?) {
        TODO("Not yet implemented")
    }

    override fun mouseClicked(e: MouseEvent) {
        if (e.isShiftDown && !e.isControlDown && e.clickCount == 1) {
            val textViewer = TextViewer(
                title = domain?.application ?: "", text = if (domain != null && domain!!.data.isNotBlank()) {
                    domain!!.data.deserializeJsonToObject<Any>().serializeToJsonPP()
                } else {
                    text.text
                }, domain
            )
            textViewer.isVisible = true
        } else if (e.isShiftDown && e.isControlDown && e.clickCount == 1) {
            Channels.kafkaChannel.put(
                PublishToTopic(
                    topic = domain!!.topic,
                    key = domain!!.key,
                    value = domain!!.data
                )
            )
        } else if (e.isMetaDown && State.onMac || e.isControlDown && !State.onMac) {
            inputTextLine.text = text.getHighlightedText()
            inputTextLine.cursorIndex = inputTextLine.text.length

            Channels.searchChannel.trySendBlocking(
                QueryChanged(
                    text.getHighlightedText(),
                    length = State.length.get(),
                    offset = State.offset.get(),
                    levels = State.levels.get()
                )
            )
        }
    }

    override fun mousePressed(e: MouseEvent?) {
        TODO("Not yet implemented")
    }

    override fun mouseReleased(e: MouseEvent?) {
        TODO("Not yet implemented")
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

    override fun mouseWheelMoved(e: MouseWheelEvent?) {
        TODO("Not yet implemented")
    }

    override fun mouseDragged(e: MouseEvent?) {
        TODO("Not yet implemented")
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y
        text.highlightRange =
            if (e.isMetaDown && State.onMac || e.isControlDown && !State.onMac) {
                getWordRangeAtIndex(text.text, getCharIndexFromMouse(text.text, mouseposX))
            } else {
                null
            }
        parent.repaint(this)
    }

    private fun getCharIndexFromMouse(string: String, x: Int): Int {
        var s = ""
        string.forEachIndexed { i, c ->
            s += c
            val sw = g2d.fontMetrics.stringWidth(s)
            if (sw > x) {
                return i
            }
        }
        return string.lastIndex
    }

    fun getWordRangeAtIndex(input: String, charIndex: Int): IntRange? {
        val words = input.split(" ", "\"") // Split the string into words based on space delimiter

        var currentCharIndex = 0
        for (word in words) {
            val wordLength = word.length
            val wordStartIndex = currentCharIndex
            val wordEndIndex = currentCharIndex + wordLength - 1

            if (charIndex in wordStartIndex..wordEndIndex) {
                val intRange = IntRange(wordStartIndex, wordEndIndex)
                assert(input.substring(intRange) == word)
                return intRange
            }

            currentCharIndex += wordLength + 1 // +1 to account for the space between words
        }

        return null // Return null if the charIndex is out of bounds
    }
}
