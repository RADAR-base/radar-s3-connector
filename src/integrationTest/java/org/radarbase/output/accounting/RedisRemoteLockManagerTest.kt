package org.radarbase.output.accounting

import org.hamcrest.MatcherAssert.assertThat
import org.hamcrest.Matchers.*
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import redis.clients.jedis.JedisPool

internal class RedisRemoteLockManagerTest {
    private lateinit var redisPool: JedisPool
    private lateinit var lockManager1: RemoteLockManager
    private lateinit var lockManager2: RemoteLockManager

    @BeforeEach
    fun setUp() {
        redisPool = JedisPool()
        lockManager1 = RedisRemoteLockManager(redisPool, "locks")
        lockManager2 = RedisRemoteLockManager(redisPool, "locks")
    }

    @AfterEach
    fun tearDown() {
        redisPool.close()
    }

    @Test
    fun testExclusiveLock() {
        lockManager1.acquireTopicLock("t").use { l1 ->
            assertThat(l1, not(nullValue()))
            lockManager2.acquireTopicLock("t").use { l2 ->
                assertThat(l2, nullValue())
            }
        }
    }

    @Test
    fun testGranularityLock() {
        lockManager1.acquireTopicLock("t1").use { l1 ->
            assertThat(l1, not(nullValue()))
            lockManager2.acquireTopicLock("t2").use { l2 ->
                assertThat(l2, not(nullValue()))
            }
        }
    }

    @Test
    fun testNonOverlappingLock() {
        lockManager1.acquireTopicLock("t").use { l1 ->
            assertThat(l1, not(nullValue()))
        }
        lockManager2.acquireTopicLock("t").use { l2 ->
            assertThat(l2, not(nullValue()))
        }
    }


    @Test
    fun testNonOverlappingLockSameManager() {
        lockManager1.acquireTopicLock("t").use { l1 ->
            assertThat(l1, not(nullValue()))
        }
        lockManager1.acquireTopicLock("t").use { l2 ->
            assertThat(l2, not(nullValue()))
        }
    }
}
