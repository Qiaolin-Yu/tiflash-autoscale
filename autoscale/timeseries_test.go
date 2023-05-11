package autoscale

import (
	"container/list"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func assertEqualFloat(t *testing.T, a float64, b float64) {
	eps := 0.0000001
	assert.True(t, math.Abs(a-b) < eps)
}

func TestAvgSigma(t *testing.T) {
	InitTestEnv()
	var avgSigma1 AvgSigma
	avgSigma1.Add(1.0)
	avgSigma1.Reset()
	assertEqual(t, avgSigma1.Sum(), 0.0)
	assertEqual(t, avgSigma1.Cnt(), int64(0))
	assertEqual(t, avgSigma1.Avg(), 0.0)

	avgSigma1.Add(1.0)
	assertEqual(t, avgSigma1.Sum(), 1.0)
	assertEqual(t, avgSigma1.Cnt(), int64(1))
	assertEqual(t, avgSigma1.Avg(), 1.0)

	avgSigma1.Sub(2.0)
	assertEqual(t, avgSigma1.Sum(), -1.0)
	assertEqual(t, avgSigma1.Cnt(), int64(0))
	assertEqual(t, avgSigma1.Avg(), 0.0)

	avgSigma1.Add(9.0)
	assertEqual(t, avgSigma1.Cnt(), int64(1))
	assertEqual(t, avgSigma1.Sum(), 8.0)
	assertEqual(t, avgSigma1.Avg(), 8.0)

	avgSigma1.Add(7.0)
	assertEqual(t, avgSigma1.Cnt(), int64(2))
	assertEqual(t, avgSigma1.Sum(), 15.0)
	assertEqual(t, avgSigma1.Avg(), 7.5)

	var avgSigma2 AvgSigma
	avgSigma2.Add(1.0)
	avgSigma2.Add(2.0)
	avgSigma2.Add(3.0)
	assertEqual(t, avgSigma2.Cnt(), int64(3))
	assertEqual(t, avgSigma2.Sum(), 6.0)
	assertEqual(t, avgSigma2.Avg(), 2.0)

	avgSigma1.Merge(&avgSigma2)
	assertEqual(t, avgSigma1.Cnt(), int64(5))
	assertEqual(t, avgSigma1.Sum(), 21.0)
	assertEqual(t, avgSigma1.Avg(), 4.2)

	var avgSigma3 AvgSigma
	avgSigma3.Add(12.0)
	avgSigma3.Add(13.0)
	assertEqual(t, avgSigma3.Cnt(), int64(2))
	assertEqual(t, avgSigma3.Sum(), 25.0)
	assertEqual(t, avgSigma3.Avg(), 12.5)

	var avgSigma4 AvgSigma
	avgSigma4.Add(20)
	assertEqual(t, avgSigma4.Cnt(), int64(1))
	assertEqual(t, avgSigma4.Sum(), 20.0)
	assertEqual(t, avgSigma4.Avg(), 20.0)

	avgSigmaArray1 := []AvgSigma{avgSigma1, avgSigma2}
	avgSigmaArray2 := []AvgSigma{avgSigma3, avgSigma4}

	Merge(avgSigmaArray1, avgSigmaArray2)
	assertEqual(t, avgSigmaArray1[0].Cnt(), int64(7))
	assertEqual(t, avgSigmaArray1[0].Sum(), 46.0)
	assertEqualFloat(t, avgSigmaArray1[0].Avg(), 6.571428571428571)
	assertEqual(t, avgSigmaArray1[1].Cnt(), int64(4))
	assertEqual(t, avgSigmaArray1[1].Sum(), 26.0)
	assertEqual(t, avgSigmaArray1[1].Avg(), 6.5)

	temp := Avg(avgSigmaArray1)
	assertEqual(t, temp[0], 0.0)
	assertEqual(t, temp[1], 0.0)
	assertEqual(t, temp[2], 0.0)
	assertEqualFloat(t, temp[3], 6.571428571428571)
	assertEqual(t, temp[4], 6.5)

	arr := []float64{1000.0, 2.0}
	Add(avgSigmaArray1, arr)
	assertEqual(t, avgSigmaArray1[0].Cnt(), int64(8))
	assertEqual(t, avgSigmaArray1[0].Sum(), 1046.0)
	assertEqual(t, avgSigmaArray1[0].Avg(), 130.75)
	assertEqual(t, avgSigmaArray1[1].Cnt(), int64(5))
	assertEqual(t, avgSigmaArray1[1].Sum(), 28.0)
	assertEqual(t, avgSigmaArray1[1].Avg(), 5.6)
}

func TestSimpleTimeSeries(t *testing.T) {
	InitTestEnv()
	cfgIntervalSec := 2
	simpleTimeSeries := &SimpleTimeSeries{
		series:      list.New(),
		Statistics:  make([]AvgSigma, CapacityOfStaticsAvgSigma),
		lastStat:    make([]AvgSigma, CapacityOfStaticsAvgSigma),
		cap:         computeSeriesCapBasedOnIntervalSec(cfgIntervalSec),
		intervalSec: cfgIntervalSec,
	}
	simpleTimeSeries.append(1000, []float64{1.0, 1.0})
	simpleTimeSeries.append(2000, []float64{2.0, 3.0})

	minActual, maxActual := simpleTimeSeries.getMinMaxTime()
	assertEqual(t, simpleTimeSeries.Statistics[0].Cnt(), int64(1))
	assertEqual(t, simpleTimeSeries.Statistics[0].Sum(), 2.0)
	assertEqual(t, simpleTimeSeries.Statistics[1].Cnt(), int64(1))
	assertEqual(t, simpleTimeSeries.Statistics[1].Sum(), 3.0)
	assertEqual(t, simpleTimeSeries.series.Len(), 1)
	assertEqual(t, minActual, int64(2000))
	assertEqual(t, maxActual, int64(2000))

	simpleTimeSeries.intervalSec = 30
	simpleTimeSeries.cap = computeSeriesCapBasedOnIntervalSec(simpleTimeSeries.intervalSec)
	assertEqual(t, simpleTimeSeries.cap, 3)
	simpleTimeSeries.append(3000, []float64{3.0, 4.0})
	simpleTimeSeries.append(4000, []float64{4.0, 5.0})
	simpleTimeSeries.append(5000, []float64{5.0, 6.0})
	assertEqual(t, simpleTimeSeries.Statistics[0].Cnt(), int64(1))
	assertEqual(t, simpleTimeSeries.Statistics[0].Sum(), 5.0)
	assertEqual(t, simpleTimeSeries.Statistics[1].Cnt(), int64(1))
	assertEqual(t, simpleTimeSeries.Statistics[1].Sum(), 6.0)
	assertEqual(t, simpleTimeSeries.series.Len(), 1)

	simpleTimeSeries.append(5001, []float64{6.0, 7.0})
	simpleTimeSeries.append(5002, []float64{7.0, 8.0})
	assert.Equal(t, simpleTimeSeries.Statistics[0].Cnt(), int64(3))
	assert.Equal(t, simpleTimeSeries.Statistics[0].Sum(), 18.0)
	assert.Equal(t, simpleTimeSeries.Statistics[1].Cnt(), int64(3))
	assert.Equal(t, simpleTimeSeries.Statistics[1].Sum(), 21.0)
	assert.Equal(t, simpleTimeSeries.lastStat[0].Cnt(), int64(2))
	assert.Equal(t, simpleTimeSeries.lastStat[0].Sum(), 11.0)
	assert.Equal(t, simpleTimeSeries.lastStat[1].Cnt(), int64(2))
	assert.Equal(t, simpleTimeSeries.lastStat[1].Sum(), 13.0)
	simpleTimeSeries.append(5003, []float64{8.0, 9.0})
	assertEqual(t, simpleTimeSeries.series.Len(), 3)
	assertEqual(t, simpleTimeSeries.Statistics[0].Sum(), 21.0)
	assertEqual(t, simpleTimeSeries.Statistics[1].Sum(), 24.0)
	simpleTimeSeries.append(5004, []float64{9.0, 10.0})
	assertEqual(t, simpleTimeSeries.series.Len(), 3)
	assertEqual(t, simpleTimeSeries.Statistics[0].Sum(), 24.0)
	assertEqual(t, simpleTimeSeries.Statistics[1].Sum(), 27.0)
	assertEqual(t, simpleTimeSeries.lastStat[0].Cnt(), int64(3))
	assertEqual(t, simpleTimeSeries.lastStat[0].Sum(), 21.0)
	assertEqual(t, simpleTimeSeries.lastStat[1].Sum(), 24.0)
	simpleTimeSeries.append(5005, []float64{1.0, 2.0})
	assertEqual(t, simpleTimeSeries.series.Len(), 3)
	assertEqual(t, simpleTimeSeries.Statistics[0].Sum(), 18.0)
	assertEqual(t, simpleTimeSeries.Statistics[1].Sum(), 21.0)
	assertEqual(t, simpleTimeSeries.lastStat[0].Cnt(), int64(3))
	assertEqual(t, simpleTimeSeries.lastStat[0].Sum(), 24.0)
	assertEqual(t, simpleTimeSeries.lastStat[1].Sum(), 27.0)

}

func TestTimeSeriesContainer(t *testing.T) {
	InitTestEnv()
	// client, err := NewPromClientDefault()
	// if err != nil {
	// 	t.Fatal(err)
	// }

	container := NewTimeSeriesContainer()
	metricsTopicArr := []MetricsTopic{MetricsTopicCpu, MetricsTopicTaskCnt, MetricsTopicMemQuotaExceededCnt}
	for _, metricsTopic := range metricsTopicArr {
		container.InsertWithUserCfg("test", 1, []float64{2.0, 0.0}, 1, metricsTopic)
		container.InsertWithUserCfg("test", 2, []float64{3.0, 0.0}, 1, metricsTopic)
		stats, lastStats, descOfPodTimeSeries := container.GetStatisticsOfPod("test", metricsTopic)
		assertEqual(t, stats[0].Cnt(), int64(1))
		assertEqual(t, stats[0].Sum(), 3.0)
		assertEqual(t, stats[1].Cnt(), int64(1))
		assertEqual(t, stats[1].Sum(), 0.0)
		assertEqual(t, lastStats[0].Cnt(), int64(1))
		assertEqual(t, lastStats[0].Sum(), 2.0)
		assertEqual(t, lastStats[1].Cnt(), int64(1))
		assertEqual(t, lastStats[1].Sum(), 0.0)
		assertEqual(t, descOfPodTimeSeries.Size, 1)
		assertEqual(t, descOfPodTimeSeries.MinTime, int64(2))
		assertEqual(t, descOfPodTimeSeries.MaxTime, int64(2))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).SampleCntOfVals, int64(1))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).MinTime, int64(2))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).MaxTime, int64(2))

		container.ResetMetricsOfPod("test")
		container.InsertWithUserCfg("test", 3, []float64{1.0, 2.0}, 30, metricsTopic)
		container.InsertWithUserCfg("test", 4, []float64{2.0, 3.0}, 30, metricsTopic)
		container.InsertWithUserCfg("test", 5, []float64{3.0, 4.0}, 30, metricsTopic)
		stats, lastStats, descOfPodTimeSeries = container.GetStatisticsOfPod("test", metricsTopic)
		assertEqual(t, stats[0].Cnt(), int64(3))
		assertEqual(t, stats[0].Sum(), 6.0)
		assertEqual(t, stats[1].Cnt(), int64(3))
		assertEqual(t, stats[1].Sum(), 9.0)
		assertEqual(t, lastStats[0].Cnt(), int64(2))
		assertEqual(t, lastStats[0].Sum(), 3.0)
		assertEqual(t, lastStats[1].Cnt(), int64(2))
		assertEqual(t, lastStats[1].Sum(), 5.0)
		assertEqual(t, descOfPodTimeSeries.Size, 3)
		assertEqual(t, descOfPodTimeSeries.MinTime, int64(3))
		assertEqual(t, descOfPodTimeSeries.MaxTime, int64(5))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).SampleCntOfVals, int64(3))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).MinTime, int64(3))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).MaxTime, int64(5))

		container.InsertWithUserCfg("test", 7, []float64{4.0, 5.0}, 30, metricsTopic)
		stats, lastStats, descOfPodTimeSeries = container.GetStatisticsOfPod("test", metricsTopic)
		assertEqual(t, stats[0].Cnt(), int64(3))
		assertEqual(t, stats[0].Sum(), 9.0)
		assertEqual(t, stats[1].Cnt(), int64(3))
		assertEqual(t, stats[1].Sum(), 12.0)
		assertEqual(t, lastStats[0].Cnt(), int64(3))
		assertEqual(t, lastStats[0].Sum(), 6.0)
		assertEqual(t, lastStats[1].Cnt(), int64(3))
		assertEqual(t, lastStats[1].Sum(), 9.0)
		assertEqual(t, descOfPodTimeSeries.Size, 3)
		assertEqual(t, descOfPodTimeSeries.MinTime, int64(4))
		assertEqual(t, descOfPodTimeSeries.MaxTime, int64(7))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).SampleCntOfVals, int64(3))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).MinTime, int64(4))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).MaxTime, int64(7))

		container.InsertWithUserCfg("test", 6, []float64{5.0, 6.0}, 30, metricsTopic)
		stats, lastStats, descOfPodTimeSeries = container.GetStatisticsOfPod("test", metricsTopic)
		assertEqual(t, stats[0].Cnt(), int64(3))
		assertEqual(t, stats[0].Sum(), 9.0)
		assertEqual(t, stats[1].Cnt(), int64(3))
		assertEqual(t, stats[1].Sum(), 12.0)
		assertEqual(t, lastStats[0].Cnt(), int64(3))
		assertEqual(t, lastStats[0].Sum(), 6.0)
		assertEqual(t, lastStats[1].Cnt(), int64(3))
		assertEqual(t, lastStats[1].Sum(), 9.0)
		assertEqual(t, descOfPodTimeSeries.Size, 3)
		assertEqual(t, descOfPodTimeSeries.MinTime, int64(4))
		assertEqual(t, descOfPodTimeSeries.MaxTime, int64(7))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).SampleCntOfVals, int64(3))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).MinTime, int64(4))
		assertEqual(t, container.GetSnapshotOfTimeSeries("test", metricsTopic).MaxTime, int64(7))
	}
}
