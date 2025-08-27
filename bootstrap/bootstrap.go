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
	tcam := domain.NewTransactionCommitAckManager(im)

	csClient := client.NewConfigServerClient(configuration.ConfigServerUrl)

	uiSvc := service.NewUpdateInstancesService(im)
	gaiSvc := service.NewGetAllInstancesService(csClient, im)

	// ------------- Transaction Execution Strategy ---------------
	var tm domain.TransactionExecutionStrategy
	var transactionListener listener.TransactionListener

	log.Println("Chosen broadcast strategy:", configuration.Algorithm)
	switch configuration.Algorithm {
	case "ev":
		tbc := publisher.NewZeroMQTransactionBroadcaster(im)
		tm = strategy.NewEventualTransactionManager(repo, tbc)
		transactionListener = listener.NewZeromqTransactionListener(listener.ZmqTransactionListenerDependencies{im, tm, nil, false})
		if tbc != nil {
			tbc.Initialize()
			go transactionListener.Listen()
		}
	case "rb":
		tbc := publisher.NewZeroMQTransactionBroadcaster(im)
		rbtm := strategy.NewRbTransactionManager(tbc, tcam, repo, im)
		transactionListener = listener.NewZeromqTransactionListener(listener.ZmqTransactionListenerDependencies{im, rbtm, rbtm, true})
		tm = rbtm
		go transactionListener.Listen()
	case "at":
		tbc := publisher.NewAtomicBroadcaster(configuration)
		tm = strategy.NewAtomicTransactionManager(im, repo, tbc)
		transactionListener = listener.NewZeromqAtomicTransactionListener(tm, configuration)
		if tbc != nil {
			tbc.Initialize()
			go transactionListener.Listen()
		}
	}

	// ------------------------------------------------------------

	//Starting required components
	arSvc := service.NewInstanceAutoRegisterService(csClient, im, configuration)
	arSvc.Execute()
	err := gaiSvc.Execute()
	if err != nil {
		return false, err
	}

	delSvc := service.NewDeleteEntryService(repo)
	saveSvc := service.NewSaveEntryService(tm)
	getSvc := service.NewGetEntryService(repo)
	dbEntryH := dbentry.NewDbEntryHandler(saveSvc, delSvc, getSvc)
	instanceH := dbinstance.NewDbInstanceHandler(uiSvc)
	srv := server.NewServer(dbEntryH, instanceH, configuration)

	err = srv.Run()
	if err != nil {
		return false, err
	}

	return true, nil
}
