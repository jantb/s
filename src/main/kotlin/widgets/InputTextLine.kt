package widgets

import ComponentOwn
import SlidePanel
import State
import round
import util.Styles
import util.UiColors
import java.awt.EventQueue
import java.awt.Font
import java.awt.Graphics2D
import java.awt.Toolkit
import java.awt.datatransfer.Clipboard
import java.awt.datatransfer.DataFlavor
import java.awt.datatransfer.StringSelection
import java.awt.event.*
import java.awt.geom.Rectangle2D
import java.awt.image.BufferedImage
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.swing.SwingUtilities
import kotlin.math.abs

class InputTextLine(
    private val panel: SlidePanel,
    x: Int,
    y: Int,
    width: Int,
    height: Int,
    val updateText: (text: String) -> Unit
) : ComponentOwn(),
    KeyListener,
    MouseListener, MouseWheelListener,
    MouseMotionListener {
    private val scheduler = Executors.newScheduledThreadPool(1)

    init {
        this.x = x
        this.y = y
        this.height = height
        this.width = width
        scheduler.scheduleAtFixedRate({ panel.repaint() }, 0, 500, TimeUnit.MILLISECONDS)
    }

    private var rowHeight = 12
    private lateinit var image: BufferedImage
    private lateinit var g2d: Graphics2D
    private var selectedTextRange: IntRange? = null
    private var mouseposX = 0
    private var mouseposY = 0
    private var mouseposXPressed = 0
    private var mouseposYPressed = 0
    private var mouseposXReleased = 0
    private var mouseposYReleased = 0
    private lateinit var maxCharBounds: Rectangle2D
    private var lastKeyTimeStamp = System.currentTimeMillis()
    private var selectedText = ""
    var text = ""
    var textStack = LinkedList<String>()
    var textStackIndex = -1
    var cursorIndex = 0
    override fun display(width: Int, height: Int, x: Int, y: Int): BufferedImage {
        if (this.width != width || this.height != height || this.x != x || this.y != y) {
            if (::g2d.isInitialized) {
                this.g2d.dispose()
            }
            this.image = BufferedImage(width, height, BufferedImage.TYPE_INT_RGB)
            this.g2d = this.image.createGraphics()
            this.image
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

        paintSelectedText()
        paintText()
        paintCursor()
        return image
    }

    override fun repaint(componentOwn: ComponentOwn) {
        TODO("Not yet implemented")
    }

    private fun paintText() {
        g2d.color = UiColors.defaultText
        g2d.drawString(text, 0, maxCharBounds.height.toInt())
    }

    private fun paintCursor() {
        g2d.color = UiColors.defaultCursor
        if (System.currentTimeMillis().mod(1000) > 500 || (System.currentTimeMillis() - lastKeyTimeStamp) < 500) {
            g2d.fillRect(
                ((g2d.fontMetrics.stringWidth(text.substring(0, cursorIndex.coerceIn(0..text.length)))) - 1).coerceIn(0..Int.MAX_VALUE),
                0 + g2d.fontMetrics.maxDescent,
                2,
                maxCharBounds.height.toInt()
            )
        }
    }

    private fun paintSelectedText() {
        if (text.isEmpty() || selectedTextRange == null || selectedText.isBlank()) {
            return
        }
        g2d.color = UiColors.selectionText

        g2d.fillRect(
            g2d.fontMetrics.stringWidth(text.substring(0, selectedTextRange!!.first)),
            g2d.fontMetrics.maxDescent,
            g2d.fontMetrics.stringWidth(
                selectedText
            ),
            maxCharBounds.height.toInt()
        )
    }

    override fun keyTyped(e: KeyEvent) {
        if (!Character.isISOControl(e.keyChar)) {
            insertCharAtCursorIndex(e.keyChar)
            cursorIndex(1)
            lastKeyTimeStamp = System.currentTimeMillis()
            EventQueue.invokeLater {
                this.updateText(text)
            }
            selectedText = ""
            panel.repaint()
        }
    }

    override fun keyPressed(e: KeyEvent) {

        when (e.keyCode) {
            KeyEvent.VK_RIGHT -> {
                if (e.isMetaDown && State.onMac || e.isAltDown && !State.onMac) {
                    if (textStack.isNotEmpty()) {
                        textStackIndex = (textStackIndex + 1).coerceIn(textStack.indices)
                        text = textStack[textStackIndex]
                        SwingUtilities.invokeLater {
                            updateText(text)
                        }
                    }
                } else if (e.isShiftDown) {
                    cursorIndex(1)
                    if (selectedTextRange != null) {
                        selectedText += text[cursorIndex.coerceIn(text.indices)]
                        selectedTextRange = selectedTextRange!!.first..cursorIndex
                    } else {
                        selectedText += text[cursorIndex.coerceIn(text.indices)]
                        selectedTextRange = cursorIndex - 1..cursorIndex
                    }
                } else {
                    cursorIndex(1)
                    if (selectedTextRange != null) {
                        cursorIndex = selectedTextRange!!.last + 1
                        selectedText = ""
                        selectedTextRange = null
                    }
                }
            }

            KeyEvent.VK_LEFT -> {
                if (e.isMetaDown && State.onMac || e.isAltDown && !State.onMac) {
                    if (textStack.isNotEmpty()) {
                        textStackIndex = (textStackIndex - 1).coerceIn(textStack.indices)
                        text = textStack.get(textStackIndex)
                        SwingUtilities.invokeLater {
                            updateText(text)
                        }
                    }
                } else if (e.isShiftDown) {
                    cursorIndex(-1)
                    if (selectedTextRange != null) {
                        selectedText = text[cursorIndex.coerceIn(text.indices)] + text
                        selectedTextRange = cursorIndex..selectedTextRange!!.last
                    } else {
                        selectedText = text[cursorIndex.coerceIn(text.indices)] + text
                        selectedTextRange = cursorIndex - 1 until cursorIndex
                    }
                } else {
                    cursorIndex(-1)
                    if (selectedTextRange != null) {
                        cursorIndex = selectedTextRange!!.first
                        selectedText = ""
                        selectedTextRange = null
                    }
                }
            }

            KeyEvent.VK_ENTER -> {
                while (textStack.size > textStackIndex &&  textStack.isNotEmpty()) {
                    textStack.removeLast()
                }
                if (textStack.isNotEmpty()) {
                    if (textStack.last() != text) {
                        textStack.addLast(text)
                        textStackIndex++
                    }
                } else {
                    textStack.addLast(text)
                    textStackIndex++
                }
            }

            KeyEvent.VK_BACK_SPACE -> {
                if (selectedText.isNotBlank()) {
                    text = text.substring(0, selectedTextRange!!.first) + text.substring(selectedTextRange!!.last + 1)
                    selectedTextRange = null
                    selectedText = ""
                    cursorIndex(0)
                    EventQueue.invokeLater {
                        this.updateText(text)
                    }
                } else if (cursorIndex(-1)) {
                    deleteCharAtCursorIndex()
                    EventQueue.invokeLater {
                        this.updateText(text)
                    }
                }
            }

            KeyEvent.VK_C -> {
                if (e.isMetaDown && State.onMac || e.isControlDown && !State.onMac) {
                    if (selectedText.isBlank()) {
                        return
                    }
                    val clipboard: Clipboard = Toolkit.getDefaultToolkit().systemClipboard
                    val stringSelection = StringSelection(selectedText)
                    clipboard.setContents(stringSelection, stringSelection)
                }
            }

            KeyEvent.VK_A -> {
                if (e.isMetaDown && State.onMac || e.isControlDown && !State.onMac) {
                    selectedText = text
                    selectedTextRange = text.indices
                    cursorIndex = selectedTextRange!!.last + 1
                }
            }

            KeyEvent.VK_V -> {
                if (e.isMetaDown && State.onMac || e.isControlDown && !State.onMac) {
                    val clipboard = Toolkit.getDefaultToolkit().systemClipboard
                    val flavor = DataFlavor.stringFlavor

                    if (clipboard.isDataFlavorAvailable(flavor)) {
                        try {
                            val textFromClipboard = clipboard.getData(flavor) as String

                            if (selectedText.isNotBlank()) {
                                text = text.substring(
                                    0,
                                    selectedTextRange!!.first
                                ) + textFromClipboard + text.substring(selectedTextRange!!.last + 1)
                                selectedTextRange = null
                                selectedText = ""
                                cursorIndex(0)
                                EventQueue.invokeLater {
                                    this.updateText(text)
                                }
                            } else {
                                textFromClipboard.forEach {
                                    insertCharAtCursorIndex(it)
                                    cursorIndex(1)
                                }
                            }
                            EventQueue.invokeLater {
                                this.updateText(text)
                            }
                        } catch (e: Exception) {
                            e.printStackTrace()
                        }
                    }
                }
            }
        }

        lastKeyTimeStamp = System.currentTimeMillis()
        panel.repaint()
    }


    override fun keyReleased(e: KeyEvent) {

        panel.repaint()
    }

    private fun insertCharAtCursorIndex(keyChar: Char) {
        text = text.substring(0, cursorIndex) + keyChar.toString() + text.substring(cursorIndex)
    }

    private fun deleteCharAtCursorIndex() {
        text = text.substring(
            0,
            cursorIndex.coerceIn(0..cursorIndex)
        ) + text.substring((cursorIndex + 1).coerceIn(0..text.length))
    }

    private fun cursorIndex(index: Int): Boolean {
        val cursorIndexBefore = cursorIndex
        cursorIndex += index
        cursorIndex = cursorIndex.coerceIn(0..text.length)
        return cursorIndex != cursorIndexBefore
    }

    override fun mouseClicked(e: MouseEvent) {
        if (e.clickCount == 1) {
            val indexFromMouse = getCharIndexFromMouse(text, mouseposX)
            cursorIndex = indexFromMouse
            cursorIndex(0)
        }

        if (e.clickCount == 2) {
            val indexFromMouse = getCharIndexFromMouse(text, mouseposX)
            selectedText = getWordAtIndex(text, indexFromMouse) ?: ""
            selectedTextRange = getWordRangeAtIndex(text, indexFromMouse)
            cursorIndex = selectedTextRange!!.last + 1
        }
        if (e.clickCount == 3) {
            selectedText = text
            selectedTextRange = text.indices
            cursorIndex = selectedTextRange!!.last + 1
        }
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

    private fun getWordAtIndex(input: String, charIndex: Int): String? {
        val words = input.split(" ", "\"") // Split the string into words based on space delimiter

        var currentCharIndex = 0
        for (word in words) {
            val wordLength = word.length
            if (charIndex in currentCharIndex until (currentCharIndex + wordLength)) {
                return word
            }
            currentCharIndex += wordLength + 1 // +1 to account for the space between words
        }

        return null // Return null if the charIndex is out of bounds
    }

    override fun mousePressed(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7
        mouseposXPressed = mouseposX
        mouseposYPressed = mouseposY
        mouseposXReleased = mouseposX
        mouseposYReleased = mouseposY
        selectedText = ""
        panel.repaint()
    }

    override fun mouseReleased(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y
        mouseposXReleased = mouseposX
        mouseposYReleased = mouseposY
        selectedText = getSelectedTextFromMouseIndex(
            text,
            mouseposXPressed,
            mouseposXReleased
        )
        val a = getCharIndexFromMouse(text, mouseposXPressed)
        val b = getCharIndexFromMouse(text, mouseposXReleased)

        val sorted = listOf(a, b).sorted()
        selectedTextRange = (sorted[0]..sorted[1])
        panel.repaint()
    }

    private fun getSelectedTextFromMouseIndex(string: String, mouseXPressed: Int, mouseXReleased: Int): String {
        val stringWidth = g2d.fontMetrics.stringWidth(string)
        val x =
            (mouseXPressed - if (mouseXReleased - mouseXPressed > 0) 0 else abs(mouseXReleased - mouseXPressed)).round(
                maxCharBounds.width.toInt()
            ).coerceIn(0..stringWidth)
        val width = abs(mouseXReleased - mouseXPressed).round(
            maxCharBounds.width.toInt()
        ).coerceIn(0..stringWidth - x)
        var s = ""
        var res = ""
        string.forEach { c ->
            s += c
            val sw = g2d.fontMetrics.stringWidth(s)
            if (sw > x && sw <= x + width) {
                res += c
            }
        }
        return res
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
            mouseposXPressed = 0
            mouseposYPressed = 0
            mouseposXReleased = 0
            mouseposYReleased = 0
            panel.repaint()
            return
        }


        panel.repaint()
    }

    override fun mouseDragged(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7
        mouseposXReleased = mouseposX
        mouseposYReleased = mouseposY
        cursorIndex = getCharIndexFromMouse(text, mouseposX)
        panel.repaint()
    }

    override fun mouseMoved(e: MouseEvent) {
        mouseposX = e.x - x
        mouseposY = e.y - y - 7

        panel.repaint()
    }

}
