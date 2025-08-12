package bootstrap

import (
	"KVDB/internal/application/service"
	"KVDB/internal/domain"
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
)

func Run() (bool, error) {
	flag.Parse()

	configuration := config.LoadConfig()
	w, _ := lsm_tree.NewWal(configuration.WalDirectory)
	mem := lsm_tree.NewMemtable(w)
	repo := repository.NewLSMTreeRepository(mem)
	im := domain.NewDbInstanceManager()
	ackSender := publisher.NewZeroMQCommitAckSender(im)
	tbc := publisher.NewZeroMQTransactionBroadcaster(im)
	tcam := domain.NewTransactionCommitAckManager(im)
	tm := domain.NewTransactionManager(tbc, tcam, repo, ackSender, im)
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
	transactionListener := listener.NewZeromqTransactionListener(listener.ZmqTransactionListenerDependencies{im, tm, tm})
	ackListener := listener.NewZeromqCommitAckListener(listener.ZmqCommitAckListenerDependencies{im, tm})

	arSvc.Execute()
	err := gaiSvc.Execute()
	if err != nil {
		return false, err
	}

	go ackListener.Listen()
	go transactionListener.Listen()
	err = ackSender.Initialize()
	if err != nil {
		return false, err
	}
	err = srv.Run()
	if err != nil {
		return false, err
	}

	return true, nil
}
