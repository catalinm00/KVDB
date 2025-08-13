package bootstrap

import (
	"KVDB/internal/application/service"
	"KVDB/internal/domain"
	"KVDB/internal/domain/strategy"
	"KVDB/internal/platform/client"
	"KVDB/internal/platform/config"
	"KVDB/internal/platform/messaging/zeromq/listener"
	"KVDB/internal/platform/messaging/zeromq/publisher"
	"KVDB/internal/platform/repository"
	"KVDB/internal/platform/repository/lsm_tree"
	"KVDB/internal/platform/server"
	"KVDB/internal/platform/server/handler/dbentry"
	"KVDB/internal/platform/server/handler/dbinstance"
	"flag"
	"log"
)

func Run() (bool, error) {
	flag.Parse()

	configuration := config.LoadConfig()
	w, _ := lsm_tree.NewWal(configuration.WalDirectory)
	mem := lsm_tree.NewMemtable(w)
	repo := repository.NewLSMTreeRepository(mem)
	im := domain.NewDbInstanceManager()
	tbc := publisher.NewZeroMQTransactionBroadcaster(im)
	tcam := domain.NewTransactionCommitAckManager(im)

	// ------------- Transaction Execution Strategy ---------------
	var tm domain.TransactionExecutionStrategy
	var transactionListener *listener.ZeromqTransactionListener

	log.Println("Chosen broadcast strategy:", configuration.Algorithm)
	switch configuration.Algorithm {
	case "ev":
		tm = strategy.NewEventualTransactionManager(repo, tbc)
		transactionListener = listener.NewZeromqTransactionListener(listener.ZmqTransactionListenerDependencies{im, tm, nil, false})
	case "rb":

		rbtm := strategy.NewRbTransactionManager(tbc, tcam, repo, im)
		transactionListener = listener.NewZeromqTransactionListener(listener.ZmqTransactionListenerDependencies{im, rbtm, rbtm, false})

		tm = rbtm
	}

	// ------------------------------------------------------------
	csClient := client.NewConfigServerClient(configuration.ConfigServerUrl)
	arSvc := service.NewInstanceAutoRegisterService(csClient, im, configuration)
	uiSvc := service.NewUpdateInstancesService(im)
	gaiSvc := service.NewGetAllInstancesService(csClient, im)
	delSvc := service.NewDeleteEntryService(repo)
	saveSvc := service.NewSaveEntryService(tm)
	getSvc := service.NewGetEntryService(repo)
	dbEntryH := dbentry.NewDbEntryHandler(saveSvc, delSvc, getSvc)
	instanceH := dbinstance.NewDbInstanceHandler(uiSvc)
	srv := server.NewServer(dbEntryH, instanceH, configuration)

	//Starting required components
	arSvc.Execute()
	err := gaiSvc.Execute()
	if err != nil {
		return false, err
	}

	if tbc != nil {
		tbc.Initialize()
		go transactionListener.Listen()
	}

	err = srv.Run()
	if err != nil {
		return false, err
	}

	return true, nil
}
