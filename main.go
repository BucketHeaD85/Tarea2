package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"time"

	"./proto"
	"google.golang.org/grpc"
)

//Direcciones ip relevantes
//var direcciones = [3]string{"10.6.40.246:50053", "10.6.40.247:50053", "10.6.40.248:50053"}

var direcciones = [3]string{"localhost:50052", "localhost:50053", "localhost:50054"}

//var disponibles = [3]bool{true,true,true}

type clientGRPC struct {
	conn      *grpc.ClientConn
	client    proto.ServicioSubidaClient
	chunkSize int
}

var aux int

func newClient(direccion string) (c clientGRPC, err error) {

	c.chunkSize = 256000

	c.conn, err = grpc.Dial(direccion, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("failed to start grpc connection with address %v", err)
		return
	}

	c.client = proto.NewServicioSubidaClient(c.conn)

	return
}

func (c *clientGRPC) uploadFile(ctx context.Context, f string) (err error) {
	var (
		writing = true
		buf     []byte
		n       int
		file    *os.File
		status  *proto.StatusSubida
	)

	file, err = os.Open(f)
	if err != nil {
		log.Fatalf("Error al abrir el archivo: %v", err)
		return
	}
	defer file.Close()

	stream, err := c.client.SubirArchivo(ctx)
	if err != nil {
		fmt.Printf("Failed to create upstream for file:%v", err)
		return
	}
	defer stream.CloseSend()

	//stats.StartedAt = time.Now()
	buf = make([]byte, c.chunkSize)

	req := &proto.Chunk{
		Data: &proto.Chunk_Nombre{
			Nombre: f,
		},
	}

	err = stream.Send(req)

	if err != nil {
		fmt.Printf("Error al enviar informacion sobre el archivo")
		return
	}

	for writing {
		n, err = file.Read(buf)
		if err != nil {
			if err == io.EOF {
				writing = false
				err = nil
				continue
			}

			fmt.Printf("Error al copiar archivo a buffer:%v\n", err)
			return
		}

		req := &proto.Chunk{
			Data: &proto.Chunk_Contenido{
				Contenido: buf[:n],
			},
		}

		err = stream.Send(req)
		if err != nil {
			fmt.Printf("Error al enviar chunk a traves de stream:%v\n", err)
			return
		}
		fmt.Println("Chunk enviado a data node")

	}

	//stats.FinishedAt = time.Now()

	status, err = stream.CloseAndRecv()
	if err != nil {
		fmt.Printf("Error al recibir mensaje de status:%v\n", err)
		return
	}

	if status.Codigo != proto.CodigoStatusSubida_Exitoso {
		fmt.Printf("Error al subir archivo - msg:%v\n", err)
		return
	}

	return
}

func testing(nombre string, direccion string) error {
	//newClient()
	c, err := newClient(direccion)
	fileToBeChunked := nombre
	file, err := os.Open(fileToBeChunked)

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer file.Close()

	fileInfo, _ := file.Stat()

	var fileSize int64 = fileInfo.Size()

	const fileChunk = 256000
	fmt.Println(fileChunk)

	totalPartsNum := int64(math.Ceil(float64(fileSize) / float64(fileChunk)))

	fmt.Printf("Splitting to %d pieces.\n", totalPartsNum)

	//time.Sleep(time.Duration(aux) * time.Second)

	parametro := &proto.Setup{NumChunks: totalPartsNum, Titulo: fileToBeChunked}
	c.client.AcusoEnvio(context.Background(), parametro)
	for i := int64(0); i < totalPartsNum; i++ {

		partSize := int(math.Min(fileChunk, float64(fileSize-int64(i*fileChunk))))
		partBuffer := make([]byte, partSize)

		file.Read(partBuffer)

		// write to disk
		fileName := fileToBeChunked + "_" + strconv.FormatInt(i, 10)
		_, err := os.Create(fileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// write/save buffer to disk
		ioutil.WriteFile(fileName, partBuffer, os.ModeAppend)

		fmt.Println("Split to : ", fileName)
		err = c.uploadFile(context.Background(), fileName)
		if err != nil {
			return err
		}
	}
	return nil
}

func main() {
	/*
		fmt.Println("Tiempo de espera client uploader")
		_, err := fmt.Scanf("%d", &aux)
		if err != nil {
			fmt.Println(err)
		}
	*/
	go testing("Los_120_dias_de_Sodoma-Marques_de_Sade.pdf", direcciones[0])
	go testing("El_arte_de_la_guerra-Sun_Tzu.pdf", direcciones[1])
	go testing("Dracula-Stoker_Bram.pdf", direcciones[2])
	time.Sleep(15 * time.Second)

	// just for fun, let's recombine back the chunked files in a new file
	/*
		newFileName := "NEWbigfile.pdf"
		_, err = os.Create(newFileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		//set the newFileName file to APPEND MODE!!
		// open files r and w

		file, err = os.OpenFile(newFileName, os.O_APPEND|os.O_WRONLY, os.ModeAppend)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// IMPORTANT! do not defer a file.Close when opening a file for APPEND mode!
		// defer file.Close()

		// just information on which part of the new file we are appending
		var writePosition int64 = 0

		for j := uint64(0); j < totalPartsNum; j++ {

			//read a chunk
			currentChunkFileName := "bigfile_" + strconv.FormatUint(j, 10)

			newFileChunk, err := os.Open(currentChunkFileName)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			defer newFileChunk.Close()

			chunkInfo, err := newFileChunk.Stat()

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			// calculate the bytes size of each chunk
			// we are not going to rely on previous data and constant

			var chunkSize int64 = chunkInfo.Size()
			chunkBufferBytes := make([]byte, chunkSize)

			fmt.Println("Appending at position : [", writePosition, "] bytes")
			writePosition = writePosition + chunkSize

			// read into chunkBufferBytes
			reader := bufio.NewReader(newFileChunk)
			_, err = reader.Read(chunkBufferBytes)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			// DON't USE ioutil.WriteFile -- it will overwrite the previous bytes!
			// write/save buffer to disk
			//ioutil.WriteFile(newFileName, chunkBufferBytes, os.ModeAppend)

			n, err := file.Write(chunkBufferBytes)

			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}

			file.Sync() //flush to disk

			// free up the buffer for next cycle
			// should not be a problem if the chunk size is small, but
			// can be resource hogging if the chunk size is huge.
			// also a good practice to clean up your own plate after eating

			chunkBufferBytes = nil // reset or empty our buffer

			fmt.Println("Written ", n, " bytes")

			fmt.Println("Recombining part [", j, "] into : ", newFileName)
		}

		// now, we close the newFileName
		file.Close()
	*/
}
