package dev.dodeca.core.logging

/**
 * Cypher query text, read without parsing it: for queries Neo4j could not parse, and so not
 * obfuscate. Passwords in Cypher are string literals or parameters, so masking every string literal
 * hides them whatever the query's mistake.
 */
internal object QueryText {
    const val MASK = "******"

    /** [text] with every string literal's content as [MASK]. */
    fun maskLiterals(text: String): String {
        val masked = StringBuilder(text.length)
        var last = 0
        for (literal in literals(text)) {
            masked.append(text, last, literal.first).append(MASK)
            last = literal.last + 1
        }
        return masked.append(text, last, text.length).toString()
    }

    /**
     * [message], which may quote [raw], with what it quotes as [shown] shows it: Neo4j's error
     * messages quote the query, or the line of it that failed. String literals of [raw] that
     * [shown] hides are masked wherever else they appear.
     */
    fun scrub(message: String, raw: String, shown: String): String {
        if (raw == shown) return message
        var scrubbed = message.replace(raw, shown)
        val rawLines = raw.lines()
        val shownLines = shown.lines()
        if (rawLines.size == shownLines.size) {
            for ((rawLine, shownLine) in rawLines.zip(shownLines)) {
                if (rawLine != shownLine && rawLine.isNotBlank()) {
                    scrubbed = scrubbed.replace(rawLine, shownLine)
                }
            }
        }
        for (range in literals(raw)) {
            val literal = raw.substring(range)
            // Shorter ones would mask parts of words; Neo4j asks for passwords of 8 or more.
            if (literal.length >= 4 && literal !in shown) {
                scrubbed = scrubbed.replace(literal, MASK)
            }
        }
        return scrubbed
    }

    /**
     * Where the content of each string literal of [text] is, between its quotes. An unclosed
     * literal runs to the end.
     */
    fun literals(text: String): List<IntRange> {
        val literals = mutableListOf<IntRange>()
        var i = 0
        while (i < text.length) {
            val c = text[i]
            when {
                c == '\'' || c == '"' -> {
                    val start = i + 1
                    var end = start
                    while (end < text.length && text[end] != c) {
                        end += if (text[end] == '\\') 2 else 1
                    }
                    end = minOf(end, text.length)
                    if (end > start) literals += start until end
                    i = end + 1
                }
                c == '`' -> {
                    val end = text.indexOf('`', i + 1)
                    i = if (end < 0) text.length else end + 1
                }
                text.startsWith("//", i) -> {
                    val end = text.indexOf('\n', i)
                    i = if (end < 0) text.length else end + 1
                }
                text.startsWith("/*", i) -> {
                    val end = text.indexOf("*/", i + 2)
                    i = if (end < 0) text.length else end + 2
                }
                else -> i++
            }
        }
        return literals
    }
}
