package dev.dodeca.it

import org.awaitility.Awaitility
import org.junit.jupiter.api.extension.BeforeAllCallback
import org.junit.jupiter.api.extension.ExtensionContext

/**
 * Has Awaitility evaluate conditions on the test's own thread, where a call that does not return
 * keeps the test from ending until JUnit's timeout, which prints the stack of every thread first.
 * On a thread of its own, Awaitility would give up on such a call once its time runs out and
 * interrupt it, and the test would fail without telling where it was stuck.
 */
class AwaitInTestThread : BeforeAllCallback {
    override fun beforeAll(context: ExtensionContext) {
        Awaitility.pollInSameThread()
    }
}
