package autoscale

type Job interface {
	Run()
}

//func TestAwsSns(t *testing.T) {
//	awsSnsManager, err := NewAwsSnsManager("us-east-2")
//	if awsSnsManager == nil {
//		panic(err)
//	}
//	now := time.Now()
//	ts := now.UnixNano()
//	err = awsSnsManager.TryToPublishTopology("auto-scale", ts, []string{"a"})
//	if err != nil {
//		t.Errorf("[error]Create topic failed, err: %+v", err.Error())
//		return
//	}
//}

//func test(awsSnsManager *AwsSnsManager, i int) {
//	Logger.Infof("start func: %d", i)
//	now := time.Now()
//	ts := now.UnixNano()
//	err := awsSnsManager.TryToPublishTopology(strconv.Itoa(i), ts, []string{"a"})
//	if err != nil {
//		Logger.Errorf("[error]Create topic failed, err: %+v", err.Error())
//		return
//	}
//}
//
//func TestConcurrent(t *testing.T) {
//	awsSnsManager := NewAwsSnsManager("us-east-2")
//	for i := 0; i < 50; i++ {
//		go test(awsSnsManager, i)
//	}
//	time.Sleep(20 * time.Second)
//
//}
