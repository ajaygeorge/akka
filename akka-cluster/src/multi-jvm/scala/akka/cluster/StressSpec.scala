/**
 *  Copyright (C) 2009-2012 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.cluster
import language.postfixOps
import scala.collection.immutable
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import org.scalatest.BeforeAndAfterEach
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.RootActorPath
import akka.actor.SupervisorStrategy._
import akka.actor.Terminated
import akka.cluster.ClusterEvent.ClusterMetricsChanged
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.StandardMetrics.Cpu
import akka.cluster.StandardMetrics.HeapMemory
import akka.remote.RemoteScope
import akka.remote.testkit.MultiNodeConfig
import akka.remote.testkit.MultiNodeSpec
import akka.routing.FromConfig
import akka.testkit._
import akka.testkit.TestEvent._

object StressMultiJvmSpec extends MultiNodeConfig {

  // Note that this test uses default configuration,
  // not MultiNodeClusterSpec.clusterConfig
  commonConfig(ConfigFactory.parseString("""
    akka.test.cluster-stress-spec {
      nr-of-nodes-factor = 1
      nr-of-nodes = 13
      nr-of-seed-nodes = 3
      nr-of-nodes-joining-to-seed-initally = 2
      nr-of-nodes-joining-one-by-one-small = 2
      nr-of-nodes-joining-one-by-one-large = 2
      nr-of-nodes-joining-to-one = 2
      nr-of-nodes-joining-to-seed = 2
      nr-of-nodes-leaving-one-by-one-small = 1
      nr-of-nodes-leaving-one-by-one-large = 2
      nr-of-nodes-leaving = 2
      nr-of-nodes-shutdown-one-by-one-small = 1
      nr-of-nodes-shutdown-one-by-one-large = 2
      nr-of-nodes-shutdown = 2
      work-batch-size = 100
      work-batch-interval = 2s
      payload-size = 1000
      duration-factor = 1
      normal-throughput-duration = 30s
      high-throughput-duration = 10s
      supervision-duration = 10s
      report-metrics-interval = 10s
    }

    akka.actor.provider = akka.cluster.ClusterActorRefProvider
    akka.cluster {
      auto-join = off
      auto-down = on
      publish-stats-interval = 0 s # always, when it happens
    }
    akka.event-handlers = ["akka.testkit.TestEventListener"]
    akka.loglevel = INFO
    akka.remote.log-remote-lifecycle-events = off

    akka.actor.deployment {
      /master-node-1/workers {
        router = round-robin
        nr-of-instances = 100
        cluster {
          enabled = on
          max-nr-of-instances-per-node = 1
          allow-local-routees = off
        }
      }
      /master-node-2/workers {
        router = round-robin
        nr-of-instances = 100
        cluster {
          enabled = on
          routees-path = "/user/worker"
          allow-local-routees = off
        }
      }
      /master-node-3/workers = {
        router = adaptive
        nr-of-instances = 100
        cluster {
          enabled = on
          max-nr-of-instances-per-node = 1
          allow-local-routees = off
        }
      }
    }
    """))

  class Settings(conf: Config) {
    private val testConfig = conf.getConfig("akka.test.cluster-stress-spec")
    import testConfig._

    private def getDuration(name: String): FiniteDuration = Duration(getMilliseconds(name), MILLISECONDS)

    val numberOfNodesFactor = getInt("nr-of-nodes-factor")
    val totalNumberOfNodes = getInt("nr-of-nodes") * numberOfNodesFactor ensuring (
      _ >= 10, "nr-of-nodes must be >= 10")
    val numberOfSeedNodes = getInt("nr-of-seed-nodes") // not multiplied by numberOfNodesFactor
    val numberOfNodesJoiningToSeedNodesInitially =
      getInt("nr-of-nodes-joining-to-seed-initally") * numberOfNodesFactor
    val numberOfNodesJoiningOneByOneSmall =
      getInt("nr-of-nodes-joining-one-by-one-small") * numberOfNodesFactor
    val numberOfNodesJoiningOneByOneLarge =
      getInt("nr-of-nodes-joining-one-by-one-large") * numberOfNodesFactor
    val numberOfNodesJoiningToOneNode =
      getInt("nr-of-nodes-joining-to-one") * numberOfNodesFactor
    val numberOfNodesJoiningToSeedNodes =
      getInt("nr-of-nodes-joining-to-seed") * numberOfNodesFactor
    val numberOfNodesLeavingOneByOneSmall =
      getInt("nr-of-nodes-leaving-one-by-one-small") * numberOfNodesFactor
    val numberOfNodesLeavingOneByOneLarge =
      getInt("nr-of-nodes-leaving-one-by-one-large") * numberOfNodesFactor
    val numberOfNodesLeaving =
      getInt("nr-of-nodes-leaving") * numberOfNodesFactor
    val numberOfNodesShutdownOneByOneSmall =
      getInt("nr-of-nodes-shutdown-one-by-one-small") * numberOfNodesFactor
    val numberOfNodesShutdownOneByOneLarge =
      getInt("nr-of-nodes-shutdown-one-by-one-large") * numberOfNodesFactor
    val numberOfNodesShutdown =
      getInt("nr-of-nodes-shutdown") * numberOfNodesFactor

    val workBatchSize = getInt("work-batch-size")
    val workBatchInterval = Duration(getMilliseconds("work-batch-interval"), MILLISECONDS)
    val payloadSize = getInt("payload-size")
    val durationFactor = getInt("duration-factor")
    val normalThroughputDuration = getDuration("normal-throughput-duration") * durationFactor
    val highThroughputDuration = getDuration("high-throughput-duration") * durationFactor
    val supervisionDuration = getDuration("supervision-duration") * durationFactor
    val reportMetricsInterval = getDuration("report-metrics-interval")

    require(numberOfSeedNodes + numberOfNodesJoiningToSeedNodesInitially + numberOfNodesJoiningOneByOneSmall +
      numberOfNodesJoiningOneByOneLarge + numberOfNodesJoiningToOneNode + numberOfNodesJoiningToSeedNodes <= totalNumberOfNodes,
      s"specified number of joining nodes <= ${totalNumberOfNodes}")

    // don't shutdown the 3 nodes hosting the master actors
    require(numberOfNodesLeavingOneByOneSmall + numberOfNodesLeavingOneByOneLarge + numberOfNodesLeaving +
      numberOfNodesShutdownOneByOneSmall + numberOfNodesShutdownOneByOneLarge + numberOfNodesShutdown <= totalNumberOfNodes - 3,
      s"specified number of leaving/shutdown nodes <= ${totalNumberOfNodes - 3}")
  }

  // FIXME configurable number of nodes
  for (n ← 1 to 13) role("node-" + n)

  implicit class FormattedDouble(val d: Double) extends AnyVal {
    def form: String = d.formatted("%.2f")
  }

  case class ClusterResult(
    address: Address,
    duration: Duration,
    clusterStats: ClusterStats)

  class ClusterResultAggregator(title: String, expectedResults: Int, reportMetricsInterval: FiniteDuration) extends Actor with ActorLogging {
    val cluster = Cluster(context.system)
    var results = Vector.empty[ClusterResult]
    var nodeMetrics = Set.empty[NodeMetrics]
    var phiValuesObservedByNode = {
      import akka.cluster.Member.addressOrdering
      immutable.SortedMap.empty[Address, Set[PhiValue]]
    }

    import context.dispatcher
    val reportMetricsTask = context.system.scheduler.schedule(
      reportMetricsInterval, reportMetricsInterval, self, ReportTick)

    // subscribe to ClusterMetricsChanged, re-subscribe when restart
    override def preStart(): Unit = cluster.subscribe(self, classOf[ClusterMetricsChanged])
    override def postStop(): Unit = {
      cluster.unsubscribe(self)
      reportMetricsTask.cancel()
      super.postStop()
    }

    def receive = {
      case ClusterMetricsChanged(clusterMetrics) ⇒ nodeMetrics = clusterMetrics
      case PhiResult(from, phiValues)            ⇒ phiValuesObservedByNode += from -> phiValues
      case ReportTick ⇒
        log.info(s"[${title}] in progress\n${formatMetrics}\n${formatPhi}")
      case r: ClusterResult ⇒
        results :+= r
        if (results.size == expectedResults) {
          log.info(s"[${title}] completed in [${maxDuration.toMillis}] ms\n${totalClusterStats}\n${formatMetrics}\n${formatPhi}")
          context stop self
        }
      case _: CurrentClusterState ⇒
    }

    def maxDuration = results.map(_.duration).max

    def totalClusterStats = results.map(_.clusterStats).foldLeft(ClusterStats()) { (acc, s) ⇒
      ClusterStats(
        receivedGossipCount = acc.receivedGossipCount + s.receivedGossipCount,
        mergeConflictCount = acc.mergeConflictCount + s.mergeConflictCount,
        mergeCount = acc.mergeCount + s.mergeCount,
        mergeDetectedCount = acc.mergeDetectedCount + s.mergeDetectedCount)
    }

    def formatMetrics: String = {
      import akka.cluster.Member.addressOrdering
      formatMetricsHeader + "\n" +
        nodeMetrics.toSeq.sortBy(_.address).map(m ⇒ m.address + "\t" + formatMetricsLine(m)).mkString("\n")
    }

    def formatMetricsHeader: String = "Node\tHeap (MB)\tCPU (%)\tLoad"

    def formatMetricsLine(nodeMetrics: NodeMetrics): String = {
      (nodeMetrics match {
        case HeapMemory(address, timestamp, used, committed, max) ⇒
          (used.doubleValue / 1024 / 1024).form
        case _ ⇒ ""
      }) + "\t" +
        (nodeMetrics match {
          case Cpu(address, timestamp, loadOption, cpuOption, processors) ⇒
            format(cpuOption) + "\t" + format(loadOption)
          case _ ⇒ "N/A\tN/A"
        })
    }

    def format(opt: Option[Double]) = opt match {
      case None    ⇒ "N/A"
      case Some(x) ⇒ x.form
    }

    def formatPhi: String = {
      if (phiValuesObservedByNode.isEmpty) ""
      else {
        import akka.cluster.Member.addressOrdering
        val lines =
          for {
            (monitor, phiValues) ← phiValuesObservedByNode
            phi ← phiValues.toSeq.sortBy(_.address)
          } yield formatPhiLine(monitor, phi.address, phi)

        formatPhiHeader + "\n" + lines.mkString("\n")
      }
    }

    def formatPhiHeader: String = "Monitor\tSubject\tcount phi > 1.0\tcount\tmax phi"

    def formatPhiLine(monitor: Address, subject: Address, phi: PhiValue): String = {
      monitor + "\t" + subject + "\t" + phi.countAboveOne + "\t" + phi.count + "\t" + phi.max.form
    }
  }

  class PhiObserver extends Actor with ActorLogging {
    val cluster = Cluster(context.system)
    val fd = cluster.failureDetector.asInstanceOf[AccrualFailureDetector]
    var reportTo: Option[ActorRef] = None
    var phiByNode = Map.empty[Address, PhiValue].withDefault(address ⇒ PhiValue(address, 0, 0, 0.0))
    var nodes = Set.empty[Address]

    import context.dispatcher
    val checkPhiTask = context.system.scheduler.schedule(
      1.second, 1.second, self, PhiTick)

    // subscribe to MemberEvent, re-subscribe when restart
    override def preStart(): Unit = cluster.subscribe(self, classOf[MemberEvent])
    override def postStop(): Unit = {
      cluster.unsubscribe(self)
      checkPhiTask.cancel()
      super.postStop()
    }

    def receive = {
      case PhiTick ⇒
        nodes foreach { node ⇒
          val previous = phiByNode(node)
          val φ = fd.phi(node)
          if (φ > 0) {
            val aboveOne = if (!φ.isInfinite && φ > 1.0) 1 else 0
            phiByNode += node -> PhiValue(node, previous.countAboveOne + aboveOne, previous.count + 1,
              math.max(previous.max, φ))
          }
        }
        reportTo foreach { _ ! PhiResult(cluster.selfAddress, phiByNode.values.toSet) }
      case state: CurrentClusterState ⇒
        nodes = state.members.map(_.address) ++ state.unreachable.map(_.address)
      case memberEvent: MemberEvent ⇒ nodes += memberEvent.member.address
      case a: ActorRef              ⇒ reportTo = Some(a)
    }
  }

  class Master(settings: StressMultiJvmSpec.Settings, batchInterval: FiniteDuration) extends Actor {
    val workers = context.actorOf(Props[Worker].withRouter(FromConfig), "workers")
    val payload = Array.fill(settings.payloadSize)(ThreadLocalRandom.current.nextInt(127).toByte)
    var idCounter = 0L
    def nextId(): JobId = {
      idCounter += 1
      idCounter
    }
    var sendCounter = 0L
    var ackCounter = 0L
    var outstanding = Map.empty[JobId, JobState]
    var startTime = 0L

    import context.dispatcher
    val resendTask = context.system.scheduler.schedule(3.seconds, 3.seconds, self, RetryTick)

    override def postStop(): Unit = {
      resendTask.cancel()
      super.postStop()
    }

    def receive = {
      case Begin ⇒
        startTime = System.nanoTime
        self ! SendBatch
        context.become(working)
      case RetryTick ⇒
    }

    def working: Receive = {
      case Ack(id) ⇒
        outstanding -= id
        ackCounter += 1
        if (outstanding.size == settings.workBatchSize / 2)
          if (batchInterval == Duration.Zero) self ! SendBatch
          else context.system.scheduler.scheduleOnce(batchInterval, self, SendBatch)
      case SendBatch ⇒
        if (outstanding.size < settings.workBatchSize)
          sendJobs()
      case RetryTick ⇒ resend()
      case End ⇒
        done(sender)
        context.become(ending(sender))
    }

    def ending(replyTo: ActorRef): Receive = {
      case Ack(id) ⇒
        outstanding -= id
        ackCounter += 1
        done(replyTo)
      case SendBatch ⇒
      case RetryTick ⇒ resend()
    }

    def done(replyTo: ActorRef): Unit = if (outstanding.isEmpty) {
      val duration = (System.nanoTime - startTime).nanos
      replyTo ! WorkResult(duration, sendCounter, ackCounter)
      context stop self
    }

    def sendJobs(): Unit = {
      0 until settings.workBatchSize foreach { _ ⇒
        send(Job(nextId(), payload))
      }
    }

    def resend(): Unit = {
      outstanding.values foreach { jobState ⇒
        if (jobState.deadline.isOverdue)
          send(jobState.job)
      }
    }

    def send(job: Job): Unit = {
      outstanding += job.id -> JobState(Deadline.now + 5.seconds, job)
      sendCounter += 1
      workers ! job
    }
  }

  class Worker extends Actor {
    override def postStop(): Unit = {
      super.postStop()
    }
    def receive = {
      case Job(id, payload) ⇒ sender ! Ack(id)
    }
  }

  class Watchee extends Actor {
    def receive = Actor.emptyBehavior
  }

  class Supervisor extends Actor {

    var restartCount = 0

    override val supervisorStrategy =
      OneForOneStrategy(maxNrOfRetries = 5, withinTimeRange = 1 minute) {
        case _: Exception ⇒
          restartCount += 1
          Restart
      }

    def receive = {
      case props: Props     ⇒ context.actorOf(props)
      case e: Exception     ⇒ context.children foreach { _ ! e }
      case GetChildrenCount ⇒ sender ! ChildrenCount(context.children.size, restartCount)
      case ResetRestartCount ⇒
        require(context.children.isEmpty,
          s"ResetChildrenCount not allowed when children exists, [${context.children.size}]")
        restartCount = 0
    }
  }

  class RemoteChild extends Actor {
    def receive = {
      case e: Exception ⇒ throw e
    }
  }

  case object Begin
  case object End
  case object RetryTick
  case object ReportTick
  case object PhiTick
  case class PhiResult(from: Address, phiValues: Set[PhiValue])
  case class PhiValue(address: Address, countAboveOne: Int, count: Int, max: Double)

  type JobId = Long
  case class Job(id: JobId, payload: Any)
  case class Ack(id: JobId)
  case class JobState(deadline: Deadline, job: Job)
  case class WorkResult(duration: Duration, sendCount: Long, ackCount: Long) {
    def droppedCount: Long = sendCount - ackCount
    def messagesPerSecond: Double = ackCount * 1000.0 / duration.toMillis
  }
  case object SendBatch

  case object GetChildrenCount
  case class ChildrenCount(numberOfChildren: Int, numberOfChildRestarts: Int)
  case object ResetRestartCount

}

class StressMultiJvmNode1 extends StressSpec
class StressMultiJvmNode2 extends StressSpec
class StressMultiJvmNode3 extends StressSpec
class StressMultiJvmNode4 extends StressSpec
class StressMultiJvmNode5 extends StressSpec
class StressMultiJvmNode6 extends StressSpec
class StressMultiJvmNode7 extends StressSpec
class StressMultiJvmNode8 extends StressSpec
class StressMultiJvmNode9 extends StressSpec
class StressMultiJvmNode10 extends StressSpec
class StressMultiJvmNode11 extends StressSpec
class StressMultiJvmNode12 extends StressSpec
class StressMultiJvmNode13 extends StressSpec

abstract class StressSpec
  extends MultiNodeSpec(StressMultiJvmSpec)
  with MultiNodeClusterSpec with BeforeAndAfterEach with ImplicitSender {

  import StressMultiJvmSpec._
  import ClusterEvent._

  val settings = new Settings(system.settings.config)
  import settings._

  var step = 0
  var usedRoles = 0

  override def beforeEach(): Unit = { step += 1 }

  override def muteLog(sys: ActorSystem = system): Unit = {
    super.muteLog(sys)
    sys.eventStream.publish(Mute(EventFilter[RuntimeException](pattern = ".*Simulated exception.*")))
    // FIXME mute more things
  }

  val seedNodes = roles.take(numberOfSeedNodes)

  override def cluster: Cluster = {
    createWorker
    super.cluster
  }

  // always create one worker when the cluster is started
  lazy val createWorker: Unit =
    system.actorOf(Props[Worker], "worker")

  def createResultAggregator(title: String, expectedResults: Int): Unit = {
    runOn(roles.head) {
      system.actorOf(Props(new ClusterResultAggregator(title, expectedResults, reportMetricsInterval)),
        name = "result" + step)
    }
    enterBarrier("result-aggregator-created-" + step)
    phiObserver ! clusterResultAggregator
  }

  def clusterResultAggregator: ActorRef = system.actorFor(node(roles.head) / "user" / ("result" + step))

  lazy val phiObserver = system.actorOf(Props[PhiObserver], "phiObserver")

  def awaitClusterResult: Unit = {
    runOn(roles.head) {
      val r = clusterResultAggregator
      watch(r)
      expectMsgPF(remaining) { case Terminated(`r`) ⇒ true }
    }
    enterBarrier("cluster-result-done-" + step)
  }

  def joinOneByOne(numberOfNodes: Int): Unit = {
    0 until numberOfNodes foreach { _ ⇒
      joinOne()
      usedRoles += 1
      step += 1
    }
  }

  def joinOne(): Unit = within(5.seconds + 2.seconds * (usedRoles + 1)) {
    val currentRoles = roles.take(usedRoles + 1)
    createResultAggregator(s"join one to ${usedRoles} nodes cluster", currentRoles.size)
    runOn(currentRoles: _*) {
      reportResult {
        runOn(currentRoles.last) {
          cluster.join(roles.head)
        }
        awaitUpConvergence(currentRoles.size, timeout = remaining)
      }

    }
    awaitClusterResult
    enterBarrier("join-one-" + step)
  }

  def joinSeveral(numberOfNodes: Int, toSeedNodes: Boolean): Unit =
    within(10.seconds + 2.seconds * (usedRoles + numberOfNodes)) {
      val currentRoles = roles.take(usedRoles + numberOfNodes)
      val joiningRoles = currentRoles.takeRight(numberOfNodes)
      val title = s"join ${numberOfNodes} to ${if (toSeedNodes) "seed nodes" else "one node"}, in ${usedRoles} nodes cluster"
      createResultAggregator(title, currentRoles.size)
      runOn(currentRoles: _*) {
        reportResult {
          runOn(joiningRoles: _*) {
            if (toSeedNodes) cluster.joinSeedNodes(seedNodes.toIndexedSeq map address)
            else cluster.join(roles.head)
          }
          awaitUpConvergence(currentRoles.size, timeout = remaining)
        }

      }
      awaitClusterResult
      enterBarrier("join-several-" + step)
    }

  def removeOneByOne(numberOfNodes: Int, shutdown: Boolean): Unit = {
    0 until numberOfNodes foreach { _ ⇒
      removeOne(shutdown)
      usedRoles -= 1
      step += 1
    }
  }

  def removeOne(shutdown: Boolean): Unit = within(10.seconds + 2.seconds * (usedRoles - 1)) {
    val currentRoles = roles.take(usedRoles - 1)
    createResultAggregator(s"${if (shutdown) "shutdown" else "remove"} one from ${usedRoles} nodes cluster", currentRoles.size)
    val removeRole = roles(usedRoles - 1)
    val removeAddress = address(removeRole)
    runOn(removeRole) {
      system.actorOf(Props[Watchee], "watchee")
      if (!shutdown) cluster.leave(myself)
    }
    enterBarrier("watchee-created-" + step)
    runOn(roles.head) {
      // FIXME enable this check when AdressTerminated is published when MemberRemoved
      //watch(system.actorFor(node(removeRole) / "user" / "watchee"))
    }
    enterBarrier("watch-estabilished-" + step)

    runOn(currentRoles: _*) {
      reportResult {
        runOn(roles.head) {
          if (shutdown) testConductor.shutdown(removeRole, 0).await
        }
        awaitUpConvergence(currentRoles.size, timeout = remaining)
      }
    }

    runOn(roles.head) {
      val expectedRef = system.actorFor(RootActorPath(removeAddress) / "user" / "watchee")
      // FIXME enable this check when AdressTerminated is published when MemberRemoved
      //      expectMsgPF(remaining) {
      //        case Terminated(`expectedRef`) ⇒ true
      //      }
    }
    enterBarrier("watch-verified-" + step)

    awaitClusterResult
    enterBarrier("remove-one-" + step)
  }

  def removeSeveral(numberOfNodes: Int, shutdown: Boolean): Unit =
    within(10.seconds + 3.seconds * (usedRoles - numberOfNodes)) {
      val currentRoles = roles.take(usedRoles - numberOfNodes)
      val removeRoles = roles.slice(currentRoles.size, usedRoles)
      val title = s"${if (shutdown) "shutdown" else "leave"} ${numberOfNodes} in ${usedRoles} nodes cluster"
      createResultAggregator(title, currentRoles.size)
      runOn(removeRoles: _*) {
        if (!shutdown) cluster.leave(myself)
      }
      runOn(currentRoles: _*) {
        reportResult {
          runOn(roles.head) {
            if (shutdown) removeRoles.foreach { r ⇒ testConductor.shutdown(r, 0).await }
          }
          awaitUpConvergence(currentRoles.size, timeout = remaining)
        }
      }
      awaitClusterResult
      enterBarrier("remove-several-" + step)
    }

  def reportResult[T](thunk: ⇒ T): T = {
    val startTime = System.nanoTime

    val returnValue = thunk

    val duration = (System.nanoTime - startTime).nanos
    clusterResultAggregator ! ClusterResult(cluster.selfAddress, duration, clusterView.latestStats)
    returnValue
  }

  def master: ActorRef = system.actorFor("/user/master-" + myself.name)

  def exerciseRouters(title: String, duration: FiniteDuration, batchInterval: FiniteDuration, expectNoDroppedMessages: Boolean): Unit =
    within(duration + 10.seconds) {
      createResultAggregator(title, usedRoles)

      val (masterRoles, otherRoles) = roles.take(usedRoles).splitAt(3)
      runOn(masterRoles: _*) {
        reportResult {
          val m = system.actorOf(Props(new Master(settings, batchInterval)), "master-" + myself.name)
          m ! Begin
          import system.dispatcher
          system.scheduler.scheduleOnce(highThroughputDuration) {
            m.tell(End, testActor)
          }
          val workResult = awaitWorkResult
          workResult.sendCount must be > (0L)
          workResult.ackCount must be > (0L)
          if (expectNoDroppedMessages)
            workResult.droppedCount must be(0)

          enterBarrier("routers-done-" + step)
        }
      }
      runOn(otherRoles: _*) {
        reportResult {
          enterBarrier("routers-done-" + step)
        }
      }

      awaitClusterResult
    }

  def awaitWorkResult: WorkResult = {
    val m = master
    val workResult = expectMsgType[WorkResult]
    log.info("{} result, [{}] msg/s, dropped [{}] of [{}] msg", m.path.name,
      workResult.messagesPerSecond.form,
      workResult.droppedCount, workResult.sendCount)
    watch(m)
    expectMsgPF(remaining) { case Terminated(`m`) ⇒ true }
    workResult
  }

  def exerciseSupervision(title: String, duration: FiniteDuration): Unit =
    within(duration + 10.seconds) {
      val supervisor = system.actorOf(Props[Supervisor], "supervisor")
      while (remaining > 10.seconds) {
        createResultAggregator(title, usedRoles)

        reportResult {
          roles.take(usedRoles) foreach { r ⇒
            supervisor ! Props[RemoteChild].withDeploy(Deploy(scope = RemoteScope(address(r))))
          }
          supervisor ! GetChildrenCount
          expectMsgType[ChildrenCount] must be(ChildrenCount(usedRoles, 0))

          1 to 5 foreach { _ ⇒ supervisor ! new RuntimeException("Simulated exception") }
          awaitCond {
            supervisor ! GetChildrenCount
            val c = expectMsgType[ChildrenCount]
            c == ChildrenCount(usedRoles, 5 * usedRoles)
          }

          // after 5 restart attempts the children should be stopped
          supervisor ! new RuntimeException("Simulated exception")
          awaitCond {
            supervisor ! GetChildrenCount
            val c = expectMsgType[ChildrenCount]
            // zero children
            c == ChildrenCount(0, 6 * usedRoles)
          }
          supervisor ! ResetRestartCount

        }

        awaitClusterResult
      }
    }

  "A cluster under stress" must {

    "join seed nodes" taggedAs LongRunningTest in {

      val otherNodesJoiningSeedNodes = roles.slice(numberOfSeedNodes, numberOfSeedNodes + numberOfNodesJoiningToSeedNodesInitially)
      val size = seedNodes.size + otherNodesJoiningSeedNodes.size

      createResultAggregator("join seed nodes", size)

      runOn((seedNodes ++ otherNodesJoiningSeedNodes): _*) {
        reportResult {
          cluster.joinSeedNodes(seedNodes.toIndexedSeq map address)
          awaitUpConvergence(size)
        }
      }

      awaitClusterResult

      usedRoles += size
      enterBarrier("after-" + step)
    }

    "start routers that are running while nodes are joining" taggedAs LongRunningTest in {
      runOn(roles.take(3): _*) {
        system.actorOf(Props(new Master(settings, settings.workBatchInterval)), "master-" + myself.name) ! Begin
      }
      enterBarrier("after-" + step)
    }

    "join nodes one-by-one to small cluster" taggedAs LongRunningTest in {
      joinOneByOne(numberOfNodesJoiningOneByOneSmall)
      enterBarrier("after-" + step)
    }

    "join several nodes to one node" taggedAs LongRunningTest in {
      joinSeveral(numberOfNodesJoiningToOneNode, toSeedNodes = false)
      usedRoles += numberOfNodesJoiningToOneNode
      enterBarrier("after-" + step)
    }

    "join several nodes to seed nodes" taggedAs LongRunningTest in {
      joinSeveral(numberOfNodesJoiningToOneNode, toSeedNodes = true)
      usedRoles += numberOfNodesJoiningToSeedNodes
      enterBarrier("after-" + step)
    }

    "join nodes one-by-one to large cluster" taggedAs LongRunningTest in {
      joinOneByOne(numberOfNodesJoiningOneByOneLarge)
      enterBarrier("after-" + step)
    }

    "end routers that are running while nodes are joining" taggedAs LongRunningTest in within(30.seconds) {
      runOn(roles.take(3): _*) {
        val m = master
        m.tell(End, testActor)
        val workResult = awaitWorkResult
        workResult.droppedCount must be(0)
        workResult.sendCount must be > (0L)
        workResult.ackCount must be > (0L)
      }
      enterBarrier("after-" + step)
    }

    "excercise supervision" taggedAs LongRunningTest in {
      exerciseSupervision("excercise supervision", supervisionDuration)
      enterBarrier("after-" + step)
    }

    "use routers with normal throughput" taggedAs LongRunningTest in {
      exerciseRouters("use routers with normal throughput", normalThroughputDuration,
        batchInterval = workBatchInterval, expectNoDroppedMessages = true)
      enterBarrier("after-" + step)
    }

    "use routers with high throughput" taggedAs LongRunningTest in {
      exerciseRouters("use routers with high throughput", highThroughputDuration,
        batchInterval = Duration.Zero, expectNoDroppedMessages = true)
      enterBarrier("after-" + step)
    }

    "start routers that are running while nodes are removed" taggedAs LongRunningTest in {
      runOn(roles.take(3): _*) {
        system.actorOf(Props(new Master(settings, settings.workBatchInterval)), "master-" + myself.name) ! Begin
      }
      enterBarrier("after-" + step)
    }

    "leave nodes one-by-one from large cluster" taggedAs LongRunningTest in {
      removeOneByOne(numberOfNodesLeavingOneByOneLarge, shutdown = false)
      enterBarrier("after-" + step)
    }

    "shutdown nodes one-by-one from large cluster" taggedAs LongRunningTest in {
      removeOneByOne(numberOfNodesShutdownOneByOneLarge, shutdown = true)
      enterBarrier("after-" + step)
    }

    "leave several nodes" taggedAs LongRunningTest in {
      removeSeveral(numberOfNodesLeaving, shutdown = false)
      usedRoles -= numberOfNodesLeaving
      enterBarrier("after-" + step)
    }

    "shutdown several nodes" taggedAs LongRunningTest in {
      removeSeveral(numberOfNodesShutdown, shutdown = true)
      usedRoles -= numberOfNodesShutdown
      enterBarrier("after-" + step)
    }

    "leave nodes one-by-one from small cluster" taggedAs LongRunningTest in {
      removeOneByOne(numberOfNodesLeavingOneByOneSmall, shutdown = false)
      enterBarrier("after-" + step)
    }

    "shutdown nodes one-by-one from small cluster" taggedAs LongRunningTest in {
      removeOneByOne(numberOfNodesShutdownOneByOneSmall, shutdown = true)
      enterBarrier("after-" + step)
    }

    "end routers that are running while nodes are removed" taggedAs LongRunningTest in within(30.seconds) {
      runOn(roles.take(3): _*) {
        // FIXME Something is wrong with shutdown, the master/workers are not terminated, see ticket #2797
        //        val m = master
        //        m.tell(End, testActor)
        //        val workResult = awaitWorkResult
        //        workResult.sendCount must be > (0L)
        //        workResult.ackCount must be > (0L)
      }
      enterBarrier("after-" + step)
    }

  }
}
