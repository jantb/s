package widgets

import ComponentOwn
import SlidePanel
import State
import app.*
import kotlinx.coroutines.channels.trySendBlocking
import util.Styles
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
  private val panel: SlidePanel,
  x: Int,
  y: Int,
  width: Int,
  height: Int,
  private val inputTextLine: InputTextLine
) : ComponentOwn(),
  KeyListener,
  MouseListener, MouseWheelListener,
  MouseMotionListener {
  private var selectedTextRange: IntRange? = null
  private var highlightWordMouseOver: IntRange? = null
  private var indexOffset = 0
  private val scheduler = Executors.newScheduledThreadPool(1)
  private var follow = true
  private var lastUpdate = 0L

  init {
    this.x = x
    this.y = y
    this.height = height
    this.width = width
    start()
    scheduler.scheduleWithFixedDelay({
      if (State.changedAt.get() > lastUpdate) {
        updateResults()
        panel.repaint()
      }
    }, 0, 8, TimeUnit.MILLISECONDS)
  }

  private var selectedLineIndex = 0

  private var rowHeight = 12
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
        when (val msg = Channels.cmdGuiChannel.take()) {
          is ResultChanged -> updateResults(msg.result)
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
    if (this.width != width || this.height != height || this.x != x || this.y != y) {
      if (::g2d.isInitialized) {
        this.g2d.dispose()
      }
      if (height < 0) {
        this.height = 1
      } else {
        this.height = height
      }
      this.image = BufferedImage(
        width.coerceIn(1..Int.MAX_VALUE),
        this.height.coerceIn(1..Int.MAX_VALUE),
        BufferedImage.TYPE_INT_RGB
      )
      this.g2d = this.image.createGraphics()
      this.image
      this.width = width
      this.x = x
      this.y = y
      if (::maxCharBounds.isInitialized) {
        val length = if (!::maxCharBounds.isInitialized || maxCharBounds.height.toInt() == 0) {
          0
        } else {
          ((height - maxCharBounds.height.toInt()) / maxCharBounds.height.toInt()) + 1
        }
        lineList = (0..length).map {
          LineItem(
            parent = this,
            inputTextLine = inputTextLine,
            x = x,
            y = ((maxCharBounds.height.toInt()) * (it)),
            width = width,
            height = ((maxCharBounds.height.toInt()))
          )
        }
      }


      indexOffset = 0
      updateResults()
    }
    //Clear
    g2d.color = UiColors.background
    g2d.fillRect(0, 0, width, height)
    g2d.font = Font(Styles.normalFont, Font.PLAIN, rowHeight)
    maxCharBounds = g2d.fontMetrics.getMaxCharBounds(g2d)

    g2d.color = UiColors.magenta
    paintLineItem()
    return image
  }

  override fun repaint(it: ComponentOwn) {
    g2d.drawImage(it.display(it.width, it.height, it.x, it.y), it.x, it.y, it.width, it.height, null)
    panel.repaint(it.x, it.y, it.width, it.height)
  }

  private fun paintLineItem() {
    lineList
      .forEach {
        g2d.drawImage(it.display(it.width, it.height, it.x, it.y), it.x, it.y, it.width, it.height, null)
      }
  }


  override fun keyTyped(e: KeyEvent) {

  }

  override fun keyPressed(e: KeyEvent) {
    when (e.keyCode) {
      KeyEvent.VK_PAGE_UP -> {
        indexOffset += (height) / maxCharBounds.height.toInt()
        indexOffset =
          ensureIndexOffset(indexOffset)
        updateResults()
        setFollow()
      }

      KeyEvent.VK_UP -> {
        indexOffset += 1
        indexOffset =
          ensureIndexOffset(indexOffset)
        updateResults()
        setFollow()
      }

      KeyEvent.VK_PAGE_DOWN -> {
        indexOffset -= (height) / maxCharBounds.height.toInt()
        indexOffset =
          ensureIndexOffset(indexOffset)
        updateResults()
        setFollow()
      }

      KeyEvent.VK_DOWN -> {
        indexOffset -= 1
        indexOffset =
          ensureIndexOffset(indexOffset)
        updateResults()
        setFollow()
      }

      KeyEvent.VK_ENTER -> {
        indexOffset = 0
        updateResults()
        setFollow()
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

    if (e.clickCount == 1) {
      lineList.firstOrNull { mouseInside(e, it) }?.mouseClicked(e)
      updateResults()
    }

  }

  override fun mousePressed(e: MouseEvent) {
    mouseposX = e.x - x
    mouseposY = e.y - y - 7
    mouseposXPressed = mouseposX
    mouseposYPressed = mouseposY
    mouseposXReleased = mouseposX
    mouseposYReleased = mouseposY
    selectedTextRange = null
    panel.repaint()
  }

  override fun mouseReleased(e: MouseEvent) {

  }


  override fun mouseEntered(e: MouseEvent) {
    if (!mouseInside) {
      mouseInside = true
    }
    lineList.firstOrNull { mouseInside(e, it) }?.mouseEntered(e)
  }

  override fun mouseExited(e: MouseEvent) {
    if (mouseInside) {
      mouseInside = false
    }

  }

  override fun mouseWheelMoved(e: MouseWheelEvent) {
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
  }

  override fun mouseMoved(e: MouseEvent) {
    mouseposX = e.x - x
    mouseposY = e.y - y

    lineList.firstOrNull { mouseInside(e, it) }?.mouseMoved(e)
    lineList.firstOrNull { e.x in it.x..it.width && e.y in it.y..it.height }?.mouseEntered(e)
    lineList.filter { e.x !in it.x..it.width || e.y !in it.y..it.height }.forEach { it.mouseExited(e) }
  }

  private fun updateResults() {
    val length = if (!::maxCharBounds.isInitialized || maxCharBounds.height.toInt() == 0) {
      0
    } else {
      ((height - maxCharBounds.height.toInt()) / maxCharBounds.height.toInt()) + 1
    }
    State.offset.set(indexOffset)
    State.length.set(length)

    Channels.searchChannel.trySendBlocking(QueryChanged(query = inputTextLine.text, offset = indexOffset, length = length))
  }

  private fun updateResults(result: List<Domain>) {
    val length = if (!::maxCharBounds.isInitialized || maxCharBounds.height.toInt() == 0) {
      0
    } else {
      ((height - maxCharBounds.height.toInt()) / maxCharBounds.height.toInt()) + 1
    }
    if (lineList.size - 1 != length && ::maxCharBounds.isInitialized) {
      lineList = (0..length).map {
        LineItem(
          parent = this,
          inputTextLine = inputTextLine,
          x = x,
          y = ((maxCharBounds.height.toInt()) * (it)),
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
}
