package main

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
)

const (
	_ = 1 << (10 * iota)
	KB
	MB
	GB
)

type DbMemory struct {
	inBytes int
}

type MemValue map[string]DbMemory
type StrValue map[string]string

type PGSettings struct {
	dbType        string
	dbVersion     float64
	dbPlatform    string
	dbConnections int
	dbStorage     string
	dbCPU         int
	totalMemory   DbMemory
	memSettings   map[string]MemValue
	strSettings   map[string]StrValue
}

var (
	DbVersions  = []string{"9.4", "9.5", "9.6", "10", "11", "12", "13", "14"}
	DbTypes     = []string{"web", "oltp", "dw", "mixed", "desktop"}
	DbPlatforms = []string{"linux", "darwin", "windows"}
	DbStorages  = []string{"ssd", "hdd", "san"}
)

var DbDefaultValues = map[string]map[string]string{
	"9.5": {
		"max_worker_processes": "8"},
	"9.6": {
		"max_worker_processes":            "8",
		"max_parallel_workers_per_gather": "0"},
	"10": {
		"max_worker_processes":            "8",
		"max_parallel_workers_per_gather": "2",
		"max_parallel_workers":            "8"},
	"11": {
		"max_worker_processes":            "8",
		"max_parallel_workers_per_gather": "2",
		"max_parallel_workers":            "8"},
	"12": {
		"max_worker_processes":            "8",
		"max_parallel_workers_per_gather": "2",
		"max_parallel_workers":            "8"},
	"13": {
		"max_worker_processes":            "8",
		"max_parallel_workers_per_gather": "2",
		"max_parallel_workers":            "8"},
	"14": {
		"max_worker_processes":            "8",
		"max_parallel_workers_per_gather": "2",
		"max_parallel_workers":            "8"},
}

func NewDBMemory(totalMemory string) (*DbMemory, error) {
	mem := &DbMemory{}
	re := regexp.MustCompile(`^([0-9]+)([kmg]?b?)$`)
	groups := re.FindAllStringSubmatch(strings.ToLower(totalMemory), 1)
	if len(groups) > 0 {
		num, _ := strconv.Atoi(groups[0][1])
		unit := groups[0][2]
		switch {
		case strings.HasPrefix(unit, "k"):
			mem.inBytes = num * KB
		case strings.HasPrefix(unit, "m"):
			mem.inBytes = num * MB
		case strings.HasPrefix(unit, "g"):
			mem.inBytes = num * GB
		case strings.HasPrefix(unit, "b"):
			mem.inBytes = num
		default:
			mem.inBytes = num
		}

	} else {
		return nil, fmt.Errorf("can't parse memory unit: %s", totalMemory)
	}
	return mem, nil
}

func (m DbMemory) InBytes() int  { return m.inBytes }
func (m DbMemory) InKbytes() int { return m.inBytes / KB }
func (m DbMemory) InMbytes() int { return m.inBytes / MB }
func (m DbMemory) InGbytes() int { return m.inBytes / GB }

func (m DbMemory) Get() string {
	if m.InBytes()%GB == 0 {
		return fmt.Sprintf("%sGB", strconv.Itoa(m.InBytes()/GB))
	}
	if m.InBytes()%MB == 0 {
		return fmt.Sprintf("%sMB", strconv.Itoa(m.InBytes()/MB))
	}
	if m.InBytes()%KB == 0 {
		return fmt.Sprintf("%sKB", strconv.Itoa(m.InBytes()/KB))
	}
	return strconv.Itoa(m.InBytes())
}

func NewPGSettings(dbType, dbVersion, dbPlatform, totalMemory, connections,
	storage, cpu string) (*PGSettings, error) {

	pgSettings := &PGSettings{}

	if err := pgSettings.setDbType(dbType); err != nil {
		return nil, err
	}
	if err := pgSettings.setDbVersion(dbVersion); err != nil {
		return nil, err
	}
	if err := pgSettings.setDbPlatform(dbPlatform); err != nil {
		return nil, err
	}
	if err := pgSettings.setDBMemory(totalMemory); err != nil {
		return nil, err
	}
	if err := pgSettings.setDBConnections(connections); err != nil {
		return nil, err
	}
	if err := pgSettings.setDBStorage(storage); err != nil {
		return nil, err
	}
	if err := pgSettings.setDBCPU(cpu); err != nil {
		return nil, err
	}

	pgSettings.recalculate()
	return pgSettings, nil
}

func (ps *PGSettings) SetDbType(dbType string) error {
	if err := ps.setDbType(dbType); err != nil {
		return err
	}
	ps.recalculate()
	return nil
}

func (ps *PGSettings) SetDbVersion(dbVersion string) error {
	if err := ps.setDbVersion(dbVersion); err != nil {
		return err
	}
	ps.recalculate()
	return nil
}

func (ps *PGSettings) SetDbPlatform(dbPlatform string) error {
	return ps.setDbPlatform(dbPlatform)
}

func (ps *PGSettings) SetDBMemory(totalMemory string) error {
	return ps.setDBMemory(totalMemory)
}

func (ps *PGSettings) SetDBStorage(storage string) error {
	return ps.setDBStorage(storage)
}

func (ps *PGSettings) setDbType(dbType string) error {
	dbType = strings.ToLower(dbType)
	for _, t := range DbTypes {
		if t == dbType {
			ps.dbType = dbType
			return nil
		}
	}
	return fmt.Errorf("invalid type: %s", dbType)
}

func (ps *PGSettings) setDbVersion(dbVersion string) error {
	for _, v := range DbVersions {
		if v == dbVersion {
			if ver, err := strconv.ParseFloat(dbVersion, 64); err == nil {
				ps.dbVersion = ver
				return nil
			} else {
				return err
			}
		}
	}
	return fmt.Errorf("invalid version: %s", dbVersion)
}

func (ps *PGSettings) setDbPlatform(dbPlatform string) error {
	dbPlatform = strings.ToLower(dbPlatform)
	for _, p := range DbPlatforms {
		if p == dbPlatform {
			ps.dbPlatform = dbPlatform
			return nil
		}
	}
	return fmt.Errorf("invalid platform: %s", dbPlatform)
}

func (ps *PGSettings) setDBMemory(totalMemory string) error {
	mem, err := NewDBMemory(totalMemory)
	if err == nil {
		ps.totalMemory = *mem
		return nil
	} else {
		return err
	}
}

func (ps *PGSettings) setDBConnections(connections string) error {
	if connections != "" {
		if conn, err := strconv.Atoi(connections); err != nil {
			return err
		} else {
			ps.dbConnections = conn
		}
	}
	return nil
}

func (ps *PGSettings) setDBStorage(storage string) error {
	storage = strings.ToLower(storage)
	for _, s := range DbStorages {
		if s == storage {
			ps.dbStorage = storage
			return nil
		}
	}
	return fmt.Errorf("invalid storage type: %s", storage)
}

func (ps *PGSettings) setDBCPU(cpu string) error {
	if cpu != "" {
		if c, err := strconv.Atoi(cpu); err != nil {
			return err
		} else {
			ps.dbCPU = c
		}
	}
	return nil
}

func (ps PGSettings) GetMemSetting(name string) (string, error) {
	for k := range ps.memSettings {
		if k == name {
			if typeRelated := ps.memSettings[name][ps.dbType].Get(); typeRelated != "" {
				return typeRelated, nil
			}
			if storageRelated := ps.memSettings[name][ps.dbStorage].Get(); storageRelated != "" {
				return storageRelated, nil
			}
		}
	}
	return "", fmt.Errorf("invalid setting name: %s", name)
}

func (ps PGSettings) GetStrSetting(name string) (string, error) {
	for k := range ps.strSettings {
		if k == name {
			if typeRelated := ps.strSettings[name][ps.dbType]; typeRelated != "" {
				return typeRelated, nil
			}
			if storageRelated := ps.strSettings[name][ps.dbStorage]; storageRelated != "" {
				return storageRelated, nil
			}
		}
	}
	return "", fmt.Errorf("invalid setting name: %s", name)
}

func (ps PGSettings) GetSettings() map[string]string {
	settings := map[string]string{}
	for k := range ps.memSettings {
		v, _ := ps.GetMemSetting(k)
		settings[k] = v
	}
	for k := range ps.strSettings {
		v, _ := ps.GetStrSetting(k)
		settings[k] = v
	}
	return settings
}

func (ps *PGSettings) recalculate() {
	sharedBuffers := MemValue{
		"web":     DbMemory{ps.totalMemory.InBytes() / 4},
		"oltp":    DbMemory{ps.totalMemory.InBytes() / 4},
		"dw":      DbMemory{ps.totalMemory.InBytes() / 4},
		"mixed":   DbMemory{ps.totalMemory.InBytes() / 4},
		"desktop": DbMemory{ps.totalMemory.InBytes() / 16},
	}
	if ps.dbVersion < 10 && ps.dbPlatform == "windows" {
		winMemoryLimit := 512 * MB
		for dbType, mem := range sharedBuffers {
			if mem.InBytes() > winMemoryLimit {
				sharedBuffers[dbType] = DbMemory{winMemoryLimit}
			}
		}
	}

	maxConnections := StrValue{
		"web":     strconv.Itoa(200),
		"oltp":    strconv.Itoa(300),
		"dw":      strconv.Itoa(40),
		"mixed":   strconv.Itoa(100),
		"desktop": strconv.Itoa(20),
	}
	if ps.dbConnections > 0 {
		for dbType := range maxConnections {
			maxConnections[dbType] = strconv.Itoa(ps.dbConnections)
		}
	}

	effectiveCacheSize := MemValue{
		"web":     DbMemory{ps.totalMemory.InBytes() * 3 / 4},
		"oltp":    DbMemory{ps.totalMemory.InBytes() * 3 / 4},
		"dw":      DbMemory{ps.totalMemory.InBytes() * 3 / 4},
		"mixed":   DbMemory{ps.totalMemory.InBytes() * 3 / 4},
		"desktop": DbMemory{ps.totalMemory.InBytes() / 4},
	}

	maintenanceWorkMem := MemValue{
		"web":     DbMemory{ps.totalMemory.InBytes() / 16},
		"oltp":    DbMemory{ps.totalMemory.InBytes() / 16},
		"dw":      DbMemory{ps.totalMemory.InBytes() / 8},
		"mixed":   DbMemory{ps.totalMemory.InBytes() / 16},
		"desktop": DbMemory{ps.totalMemory.InBytes() / 16},
	}
	memoryLimit := 2 * GB
	for dbType := range maintenanceWorkMem {
		if maintenanceWorkMem[dbType].InBytes() > memoryLimit {
			if ps.dbPlatform == "windows" {
				maintenanceWorkMem[dbType] = DbMemory{memoryLimit - MB}
			} else {
				maintenanceWorkMem[dbType] = DbMemory{memoryLimit}
			}
		}
	}

	checkpointCompletionTarget := StrValue{
		"web":     "0.9",
		"oltp":    "0.9",
		"dw":      "0.9",
		"mixed":   "0.9",
		"desktop": "0.9",
	}

	randomPageCost := StrValue{
		"hdd": "4",
		"ssd": "1.1",
		"san": "1.1",
	}

	walBuffers := MemValue{
		"web":     DbMemory{3 * sharedBuffers["web"].InBytes() / 100},
		"oltp":    DbMemory{3 * sharedBuffers["oltp"].InBytes() / 100},
		"dw":      DbMemory{3 * sharedBuffers["dw"].InBytes() / 100},
		"mixed":   DbMemory{3 * sharedBuffers["mixed"].InBytes() / 100},
		"desktop": DbMemory{3 * sharedBuffers["desktop"].InBytes() / 100},
	}
	maxWalBuffer := 16 * MB
	walBufferNear := 14 * MB
	for dbType := range walBuffers {
		if walBuffers[dbType].InBytes() > maxWalBuffer {
			walBuffers[dbType] = DbMemory{maxWalBuffer}
		}
		if (walBuffers[dbType].InBytes() > walBufferNear) && (walBuffers[dbType].InBytes() < maxWalBuffer) {
			walBuffers[dbType] = DbMemory{maxWalBuffer}
		}
		if walBuffers[dbType].InBytes() < 32 {
			walBuffers[dbType] = DbMemory{32}
		}
	}

	defaultStatisticsTarget := StrValue{
		"web":     "100",
		"oltp":    "100",
		"dw":      "500",
		"mixed":   "100",
		"desktop": "100",
	}

	ps.memSettings = map[string]MemValue{
		"shared_buffers":       sharedBuffers,
		"effective_cache_size": effectiveCacheSize,
		"maintenance_work_mem": maintenanceWorkMem,
		"wal_buffers":          walBuffers,
	}
	ps.strSettings = map[string]StrValue{
		"max_connections":              maxConnections,
		"checkpoint_completion_target": checkpointCompletionTarget,
		"default_statistics_target":    defaultStatisticsTarget,
		"random_page_cost":             randomPageCost,
	}

	if ps.dbVersion < 9.5 {
		ps.strSettings["checkpoint_segments"] = StrValue{
			"web":     strconv.Itoa(32),
			"oltp":    strconv.Itoa(64),
			"dw":      strconv.Itoa(128),
			"mixed":   strconv.Itoa(32),
			"desktop": strconv.Itoa(3),
		}
	} else {
		ps.memSettings["min_wal_size"] = MemValue{
			"web":     DbMemory{1024 * MB},
			"oltp":    DbMemory{2048 * MB},
			"dw":      DbMemory{4096 * MB},
			"mixed":   DbMemory{1024 * MB},
			"desktop": DbMemory{100 * MB},
		}
		ps.memSettings["max_wal_size"] = MemValue{
			"web":     DbMemory{4096 * MB},
			"oltp":    DbMemory{8192 * MB},
			"dw":      DbMemory{16384 * MB},
			"mixed":   DbMemory{4096 * MB},
			"desktop": DbMemory{2048 * MB},
		}
	}

	if ps.dbPlatform == "linux" {
		ps.strSettings["effective_io_concurrency"] = StrValue{
			"hdd": "2",
			"ssd": "200",
			"san": "300",
		}
	}

	if ps.dbVersion > 9.5 && ps.dbCPU > 2 {
		ps.strSettings["max_worker_processes"] = StrValue{}
		for _, dbType := range DbTypes {
			ps.strSettings["max_worker_processes"][dbType] = strconv.Itoa(ps.dbCPU)
		}

		if ps.dbVersion >= 9.6 {
			ps.strSettings["max_parallel_workers_per_gather"] = StrValue{}
			workersPerGather := math.Ceil(float64(ps.dbCPU) / 2)
			for _, dbType := range DbTypes {
				if ps.dbType != "dw" && workersPerGather > 4 {
					ps.strSettings["max_parallel_workers_per_gather"][dbType] = strconv.Itoa(4)
				} else {
					ps.strSettings["max_parallel_workers_per_gather"][dbType] = strconv.Itoa(int(workersPerGather))
				}
			}
		}

		if ps.dbVersion >= 10 {
			ps.strSettings["max_parallel_workers"] = StrValue{}
			for _, dbType := range DbTypes {
				ps.strSettings["max_parallel_workers"][dbType] = strconv.Itoa(ps.dbCPU)
			}
		}

		if ps.dbVersion >= 11 {
			ps.strSettings["max_parallel_maintenance_workers"] = StrValue{}
			parallelMaintenanceWorkers := math.Ceil(float64(ps.dbCPU) / 2)
			if parallelMaintenanceWorkers > 4 {
				parallelMaintenanceWorkers = 4
			}
			for _, dbType := range DbTypes {
				ps.strSettings["max_parallel_maintenance_workers"][dbType] = strconv.Itoa(int(parallelMaintenanceWorkers))
			}
		}
	}

	if ps.dbVersion > 9.5 {
		var workMem int
		ps.memSettings["work_mem"] = MemValue{}
		parallelForWorkMem, _ := strconv.Atoi(DbDefaultValues[strconv.Itoa(int(ps.dbVersion))]["max_parallel_workers_per_gather"])
		if parallelForWorkMem == 0 {
			parallelForWorkMem = 1
		}
		for _, dbType := range DbTypes {
			if ps.strSettings["max_parallel_workers_per_gather"][dbType] != "" {
				parallelForWorkMem, _ = strconv.Atoi(ps.strSettings["max_parallel_workers_per_gather"][dbType])
			}
			sharedBuffersValue := ps.memSettings["shared_buffers"][dbType].InBytes()
			maxConnectionsValue, _ := strconv.Atoi(ps.strSettings["max_connections"][dbType])
			workMem = (ps.totalMemory.InBytes() - sharedBuffersValue) / (maxConnectionsValue * 3) / parallelForWorkMem
			ps.memSettings["work_mem"][dbType] = MemValue{
				"web":     DbMemory{(workMem / KB) * KB},
				"oltp":    DbMemory{(workMem / KB) * KB},
				"dw":      DbMemory{(workMem / 2 / KB) * KB},
				"mixed":   DbMemory{(workMem / 2 / KB) * KB},
				"desktop": DbMemory{(workMem / 6 / KB) * KB},
			}[dbType]
		}
	}
}
