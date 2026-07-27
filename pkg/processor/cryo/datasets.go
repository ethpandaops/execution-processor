package cryo

// datasets is every dataset the processor can write. Each descriptor lives
// beside its column mapping in the matching ds_*.go file.
var datasets = []*Dataset{
	blocksDataset,
	transactionsDataset,
	logsDataset,
	erc20TransfersDataset,
	erc721TransfersDataset,
	tracesDataset,
	nativeTransfersDataset,
	contractsDataset,
	addressAppearancesDataset,
	balanceReadsDataset,
	nonceReadsDataset,
	storageReadsDataset,
	fourByteCountsDataset,
	balanceDiffsDataset,
	nonceDiffsDataset,
	storageDiffsDataset,
}
