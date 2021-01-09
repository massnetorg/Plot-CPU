module github.com/massnetorg/Plot-CPU

require (
	github.com/google/uuid v1.1.4
	github.com/shirou/gopsutil v2.18.12+incompatible
	massnet.org/mass v1.1.1
)

replace massnet.org/mass v1.1.1 => github.com/massnetorg/MassNet-miner v1.1.1

go 1.13
