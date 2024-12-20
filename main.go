package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Measurement struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

var (
	listNames = make(chan string)
	dados     = sync.Map{}
)

func main() {
	start := time.Now()
	workers := runtime.NumCPU()
	blockSize := 100 * 1024 * 1024
	var sortList []string = []string{}

	go readLargeFileFast("measurements100.txt", workers, blockSize)

	wg := sync.WaitGroup{}
	for location := range listNames {
		wg.Add(1)
		go func(location string) {
			defer wg.Done()
			sortList = append(sortList, location)
		}(location)
	}
	wg.Wait()

	parallelSort(sortList, 4)

	fmt.Printf("{")
	for _, location := range sortList {
		value, ok := dados.Load(location)

		if !ok {
			fmt.Println("Erro ao carregar valor")
			continue
		}
		m := value.(*Measurement)
		fmt.Printf("%s=%.1f/%.1f/%.1f, ", location, m.Min, m.Max, m.Sum/float64(m.Count))
	}
	fmt.Printf("}\n")

	fmt.Println("Tempo de execução: ", time.Since(start))
}

func getRuneAndConcatenate(word rune, str *string) {
	*str += string(word)
}

func readLargeFileFast(filePath string, workers int, blockSize int) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("erro ao abrir o arquivo: %w", err)
	}
	defer file.Close()

	// Canal para enviar blocos para os workers
	blocks := make(chan []byte, workers)

	// WaitGroup para sincronizar as goroutines
	var wg sync.WaitGroup

	// Inicia workers para processar blocos
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for block := range blocks {
				processBlock(block)
			}
		}()
	}

	// Ler o arquivo em blocos grandes
	buf := make([]byte, blockSize)
	reader := bufio.NewReader(file)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			// Faz uma cópia do bloco lido para evitar problemas de sobrescrita
			block := make([]byte, n)
			copy(block, buf[:n])
			blocks <- block
		}
		if err != nil {
			break
		}
	}

	close(blocks) // Fecha o canal após terminar a leitura
	close(listNames)
	wg.Wait() // Aguarda os workers terminarem

	return nil
}

func processBlock(block []byte) {
	lines := bytes.Split(block, []byte("\n"))
	for _, line := range lines {
		processLine(string(line))
	}
}

func processLine(line string) {
	line = strings.TrimSpace(line)

	var location string
	var temp string
	var fimLocation bool
	for _, word := range line {
		if word == ';' {
			fimLocation = true
			continue
		}

		if !fimLocation {
			getRuneAndConcatenate(word, &location)
		} else {
			getRuneAndConcatenate(word, &temp)
		}
	}

	if m, ok := dados.Load(location); !ok {
		tempFloat, _ := strconv.ParseFloat(temp, 64)
		dados.Store(location, &Measurement{
			Min:   tempFloat,
			Max:   tempFloat,
			Sum:   tempFloat,
			Count: 1,
		})
		listNames <- location
	} else {
		tempFloat, _ := strconv.ParseFloat(temp, 64)

		m := m.(*Measurement)
		m.Min = min(m.Min, tempFloat)
		m.Max = max(m.Max, tempFloat)
		m.Sum += tempFloat
		m.Count++
	}

}

func parallelSort(strings []string, numWorkers int) {
	if len(strings) <= 1 || numWorkers <= 1 {
		sort.Strings(strings)
		return
	}

	// Dividir o slice em partes
	chunkSize := (len(strings) + numWorkers - 1) / numWorkers
	var wg sync.WaitGroup

	sortedChunks := make([][]string, numWorkers)
	for i := 0; i < numWorkers; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(strings) {
			end = len(strings)
		}

		wg.Add(1)
		go func(idx int, chunk []string) {
			defer wg.Done()
			sort.Strings(chunk) // Ordena cada pedaço separadamente.
			sortedChunks[idx] = chunk
		}(i, strings[start:end])
	}

	wg.Wait() // Espera todas as goroutines terminarem.

	// Mesclar os pedaços ordenados hierarquicamente
	result := mergeChunks(sortedChunks)
	copy(strings, result) // Copiar o resultado de volta ao slice original.
}

func mergeChunks(chunks [][]string) []string {
	for len(chunks) > 1 {
		var mergedChunks [][]string
		for i := 0; i < len(chunks); i += 2 {
			if i+1 < len(chunks) {
				// Mesclar dois pedaços
				mergedChunks = append(mergedChunks, mergeTwoChunks(chunks[i], chunks[i+1]))
			} else {
				// Caso seja ímpar, adicione o último pedaço diretamente
				mergedChunks = append(mergedChunks, chunks[i])
			}
		}
		chunks = mergedChunks
	}
	return chunks[0]
}

func mergeTwoChunks(a, b []string) []string {
	result := make([]string, 0, len(a)+len(b))
	i, j := 0, 0

	// Mesclagem linear
	for i < len(a) && j < len(b) {
		if a[i] <= b[j] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}

	// Adicionar os elementos restantes
	result = append(result, a[i:]...)
	result = append(result, b[j:]...)

	return result
}
