package cryo

import (
	"time"

	"github.com/ClickHouse/ch-go/proto"

	"github.com/ethpandaops/execution-processor/pkg/processor/cryo/decode"
)

var contractsDataset = &Dataset{
	Name:          "contracts",
	Table:         "canonical_execution_contracts",
	InternalIndex: true,
	MinBlock:      0,
	newSink: func(d sinkDeps) sink {
		return newDatasetSink(d, decodeContracts, func() columnar[contractRow] { return newContractColumns() })
	},
}

type contractRow struct {
	UpdatedDateTime time.Time
	BlockNumber     uint64
	TransactionHash string
	InternalIndex   uint32
	CreateIndex     uint32
	ContractAddress string
	Deployer        string
	Factory         string
	InitCode        string
	Code            proto.Nullable[string]
	InitCodeHash    string
	NInitCodeBytes  uint32
	NCodeBytes      uint32
	CodeHash        string
	MetaNetworkName string
}

func decodeContracts(t *decode.Table, meta rowMeta) ([]contractRow, error) {
	p := newPicker(t)

	var (
		blockNumber     = p.int("block_number")
		createIndex     = p.int("create_index")
		transactionHash = p.str("transaction_hash")
		contractAddress = p.str("contract_address")
		deployer        = p.str("deployer")
		factory         = p.str("factory")
		initCode        = p.str("init_code")
		code            = p.str("code")
		initCodeHash    = p.str("init_code_hash")
		nInitCodeBytes  = p.int("n_init_code_bytes")
		nCodeBytes      = p.int("n_code_bytes")
		codeHash        = p.str("code_hash")
	)

	if p.err != nil {
		return nil, p.err
	}

	idx := internalIndex(transactionHash, t.Rows())

	rows := make([]contractRow, 0, t.Rows())

	for i := range t.Rows() {
		rows = append(rows, contractRow{
			UpdatedDateTime: meta.updated,
			BlockNumber:     uintOrZero(blockNumber, i),
			TransactionHash: hexOrEmpty(transactionHash, i),
			InternalIndex:   idx[i],
			//nolint:gosec // cryo emits create_index as a parquet uint32
			CreateIndex:     uint32(uintOrZero(createIndex, i)),
			ContractAddress: hexOrEmpty(contractAddress, i),
			Deployer:        hexOrEmpty(deployer, i),
			Factory:         hexOrEmpty(factory, i),
			InitCode:        hexOrEmpty(initCode, i),
			Code:            nullableHex(code, i),
			InitCodeHash:    hexOrEmpty(initCodeHash, i),
			//nolint:gosec // cryo emits n_init_code_bytes as a parquet uint32
			NInitCodeBytes: uint32(uintOrZero(nInitCodeBytes, i)),
			//nolint:gosec // cryo emits n_code_bytes as a parquet uint32
			NCodeBytes:      uint32(uintOrZero(nCodeBytes, i)),
			CodeHash:        hexOrEmpty(codeHash, i),
			MetaNetworkName: meta.network,
		})
	}

	return rows, nil
}

type contractColumns struct {
	UpdatedDateTime proto.ColDateTime
	BlockNumber     proto.ColUInt64
	TransactionHash proto.ColFixedStr
	InternalIndex   proto.ColUInt32
	CreateIndex     proto.ColUInt32
	ContractAddress proto.ColStr
	Deployer        proto.ColStr
	Factory         proto.ColStr
	InitCode        proto.ColStr
	Code            *proto.ColNullable[string]
	InitCodeHash    proto.ColStr
	NInitCodeBytes  proto.ColUInt32
	NCodeBytes      proto.ColUInt32
	CodeHash        proto.ColStr
	MetaNetworkName *proto.ColLowCardinality[string]
}

func newContractColumns() *contractColumns {
	return &contractColumns{
		TransactionHash: proto.ColFixedStr{Size: hashSize},
		Code:            new(proto.ColStr).Nullable(),
		MetaNetworkName: new(proto.ColStr).LowCardinality(),
	}
}

func (c *contractColumns) Append(r contractRow) error {
	hash, err := fixedHash(r.TransactionHash)
	if err != nil {
		return err
	}

	c.UpdatedDateTime.Append(r.UpdatedDateTime)
	c.BlockNumber.Append(r.BlockNumber)
	c.TransactionHash.Append(hash)
	c.InternalIndex.Append(r.InternalIndex)
	c.CreateIndex.Append(r.CreateIndex)
	c.ContractAddress.Append(r.ContractAddress)
	c.Deployer.Append(r.Deployer)
	c.Factory.Append(r.Factory)
	c.InitCode.Append(r.InitCode)
	c.Code.Append(r.Code)
	c.InitCodeHash.Append(r.InitCodeHash)
	c.NInitCodeBytes.Append(r.NInitCodeBytes)
	c.NCodeBytes.Append(r.NCodeBytes)
	c.CodeHash.Append(r.CodeHash)
	c.MetaNetworkName.Append(r.MetaNetworkName)

	return nil
}

func (c *contractColumns) Reset() {
	c.UpdatedDateTime.Reset()
	c.BlockNumber.Reset()
	c.TransactionHash.Reset()
	c.InternalIndex.Reset()
	c.CreateIndex.Reset()
	c.ContractAddress.Reset()
	c.Deployer.Reset()
	c.Factory.Reset()
	c.InitCode.Reset()
	c.Code.Reset()
	c.InitCodeHash.Reset()
	c.NInitCodeBytes.Reset()
	c.NCodeBytes.Reset()
	c.CodeHash.Reset()
	c.MetaNetworkName.Reset()
}

func (c *contractColumns) Rows() int { return c.BlockNumber.Rows() }

func (c *contractColumns) Input() proto.Input {
	return proto.Input{
		{Name: "updated_date_time", Data: &c.UpdatedDateTime},
		{Name: "block_number", Data: &c.BlockNumber},
		{Name: "transaction_hash", Data: &c.TransactionHash},
		{Name: "internal_index", Data: &c.InternalIndex},
		{Name: "create_index", Data: &c.CreateIndex},
		{Name: "contract_address", Data: &c.ContractAddress},
		{Name: "deployer", Data: &c.Deployer},
		{Name: "factory", Data: &c.Factory},
		{Name: "init_code", Data: &c.InitCode},
		{Name: "code", Data: c.Code},
		{Name: "init_code_hash", Data: &c.InitCodeHash},
		{Name: "n_init_code_bytes", Data: &c.NInitCodeBytes},
		{Name: "n_code_bytes", Data: &c.NCodeBytes},
		{Name: "code_hash", Data: &c.CodeHash},
		{Name: "meta_network_name", Data: c.MetaNetworkName},
	}
}
