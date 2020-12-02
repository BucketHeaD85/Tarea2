package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"../proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type asignacion struct {
	posDireccion int64
	numChunk     int64
}

type server struct{}

//Variables para gestionar la comunicacion
var contadorChunks = int64(-1)
var totalChunks = int64(0)
var titulo string
var eleccion int
var chunkSize = 256000
var indice = 0
var contadorMensajes = int64(0)

//mutex
var mutexChunks = &sync.Mutex{}
var condChunk = sync.NewCond(mutexChunks)
var clockMutex = &sync.Mutex{}
var criticalMutex = &sync.Mutex{}

type clientGRPC struct {
	conn      *grpc.ClientConn
	client    proto.ServicioSubidaClient
	chunkSize int
}

//Conexiones entre data nodes
var clienteA clientGRPC

//var clienteB clientGRPC
//var clienteC clientGRPC

/*
var mutexNodoA = &sync.Mutex{}
var condNodoA = sync.NewCond(mutexNodoA)

var mutexNodoB = &sync.Mutex{}
var condNodoB= sync.NewCond(mutexNodoB)

var mutexNodoC = &sync.Mutex{}
var condNodoC = sync.NewCond(mutexNodoC)
*/

//Direcciones ip relevantes
var direcciones = [3]string{"10.6.40.246:50053", "10.6.40.247:50053", "10.6.40.248:50053"}
var direccionesActivas []string
//var direcciones = [3]string{"localhost:50052", "localhost:50053", "localhost:50054"}

//var disponibles = [3]bool{true,true,true}
//var nameNode = "localhost:50055"
//namenode
var nameNode = "10.6.40.249:50055"
var self string
var idDataNode int64

//Reloj lógico
var clock int64
var criticalClock = int64(0)

//algoritmo a utilizar, 0=distribuido, 1=centralizado 
var algoritmo =0

func gestionEnvios(cliente clientGRPC, nodo int, nombre string) {
	cliente = preparar(nodo)
	cliente.distribuirChunks(context.Background(), nombre)
	cliente.conn.Close()
}

func newClient(direccion string) (c clientGRPC, err error) {

	c.chunkSize = 256000

	c.conn, err = grpc.Dial(direccion, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Error al conectar a la direccion" + direccion + "\n")
		return
	}

	c.client = proto.NewServicioSubidaClient(c.conn)

	return c, err
}

func conectarDataNode(direccion int) clientGRPC {
	var tecla string
	fmt.Println("Direccion de nodo destino" + direcciones[direccion])
	c1, err := newClient(direcciones[direccion])
	for err != nil {
		//	disponibles[direccion] = false
		fmt.Println("Error al conectar al Data Node de direccion " + direcciones[direccion] + ". Presione 'r' para reintentar, o cualquier otra para omitir.")
		fmt.Scanf("%s", &tecla)
		if strings.ToLower(tecla) == "r" {
			c1, err = newClient(direcciones[direccion])
			/*
				if(err == nil){
					disponibles[direccion] = true
				}*/
		} else {
			break
		}

	}
	return c1
}

func preparar(nodo int) clientGRPC {
	cliente := conectarDataNode(nodo)
	return cliente
}

func main() {
	/*
		fmt.Println("Puerto")

		_, err := fmt.Scanf("%d", &eleccion)
		if err != nil {
			fmt.Println(err)
		}
	*/
	//escoger algoritmo
	fmt.Println("Escoger algoritmo, 0 distribuido, 1 centralizado")
	_, err := fmt.Scanf("%d", &algoritmo)
	if err != nil {
		fmt.Println(err)
	}
	eleccion = 1
	//identificadores
	var puerto = ":50053"
	self="localhost:50053"
	idDataNode=int64(1)
	clock=0
	/*
		var puerto string
		if eleccion == 0 {
			puerto = ":50052"
		} else {
			if eleccion == 1 {
				puerto = ":50053"
			} else {
				puerto = ":50054"
			}
		}
	*/
	listener, err := net.Listen("tcp", puerto)
	if err != nil {
		log.Fatalf("failed to listen on port: %v", err)
	}

	srv := grpc.NewServer()
	proto.RegisterServicioSubidaServer(srv, &server{})
	reflection.Register(srv)
	fmt.Println("Escuchando")
	if e := srv.Serve(listener); e != nil {
		log.Fatalf("failed to Serve on port: %v", e)
	}

}

func (c *clientGRPC) distribuirChunks(ctx context.Context, f string) (err error) {
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
		fmt.Printf("Failed to create upstream for file:%v\n", err)
		return
	}
	defer stream.CloseSend()

	req := &proto.Chunk{
		Data: &proto.Chunk_Nombre{
			Nombre: f,
		},
	}

	err = stream.Send(req)

	if err != nil {
		fmt.Printf("Error al enviar informacion sobre el chunk:%v\n", err)
		return
	}

	//stats.StartedAt = time.Now()
	buf = make([]byte, c.chunkSize)
	for writing {
		n, err = file.Read(buf)
		if err != nil {
			if err == io.EOF {
				writing = false
				err = nil
				continue
			}

			fmt.Printf("Error while copying from file to buf:%v\n", err)
			return
		}

		req := &proto.Chunk{
			Data: &proto.Chunk_Contenido{
				Contenido: buf[:n],
			},
		}

		err = stream.Send(req)
		fmt.Println("Chunk enviado a data node")
	}

	//stats.FinishedAt = time.Now()

	status, err = stream.CloseAndRecv()
	if err != nil {
		fmt.Printf("Error al recibir respuesta:%v\n", err)
		return
	}

	if status.Codigo != proto.CodigoStatusSubida_Exitoso {
		fmt.Printf("Error al subir archivo - msg:%v", err)
		return
	}

	return
}

func generarPropuesta(numChunks int64) error {
	defer timeTrack(time.Now(), eleccion)
	asignaciones := make([]*proto.Asignacion, numChunks)
	//fmt.Println("NumeroChunks:", numChunks)
	pos := int64(-1)
	for i := int64(0); i < numChunks; i++ {
		//dado := rand.Float64()
		asignaciones[i] = new(proto.Asignacion)
		asignaciones[i].PosDireccion = (pos + 1) % 3
		pos++
		asignaciones[i].NumChunk = i + int64(1)
		fmt.Println("Generated proposal:" + direcciones[asignaciones[i].PosDireccion])
	}

	conn, err := grpc.Dial(nameNode, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("Error al conectar al namenode: %s\n", err)
		return err
	}
	client := proto.NewServicioNameNodeClient(conn)
	propuesta := &proto.Propuesta{
		Asignacion: asignaciones,
		Titulo:     titulo,
		Nchunks:    numChunks,
		IdNodo:     int64(eleccion),
	}

	solicitud := proto.Request{
		Id: int64(eleccion),
	}
	//fmt.Println(len(propuesta.Asignacion))
	//fmt.Println("Largo de asignaciones:%v", len(asignaciones))
	//fmt.Println("Numero de chunks:", numChunks)
	

	var respuesta *proto.Propuesta
	if algoritmo !=0{
		client.SolicitarAcceso(context.Background(), &solicitud)
		respuesta,_= client.Confirmar(context.Background(), propuesta)
		contadorMensajes = respuesta.GetIdNodo()
	}else{
		propuesta:= confirmarDistribucion(propuesta)
		//walala confirmation here <-----------
		criticalMutex.Lock()
		requestCritical()
		clockMutex.Lock()
		criticalClock=int64(0)
		clockMutex.Unlock()
		respuesta, _ = client.EnviarDistribucion(context.Background(), propuesta)
		contadorMensajes += int64(2)
		criticalMutex.Unlock()
	}


	for i := int64(0); i < numChunks; i++ {
		fmt.Println("Titulo:" + titulo)
		nodo := int(respuesta.GetAsignacion()[i].PosDireccion)
		nombre := titulo + "_" + strconv.FormatInt(i, 10)
		fmt.Println("Nombre:" + nombre)
		if nodo == eleccion {
			//ioutil.WriteFile(nombre, recepcion.Contenido, os.ModeAppend)
			continue
		}
		contadorMensajes++
		gestionEnvios(clienteA, nodo, nombre)
		e := os.Remove(nombre) 
		if e != nil { 
			log.Fatal(e) 
		} 
	}
	fmt.Printf("%d mensajes enviados para la distribucion de %v\n", contadorMensajes, titulo)

	//fmt.Println(respuesta.GetAsignacion())
	return nil

}

func (server *server) AcusoEnvio(ctx context.Context, nChunks *proto.Setup) (*proto.Setup, error) {
	condChunk.L.Lock()
	contadorChunks = nChunks.GetNumChunks()
	titulo = nChunks.GetTitulo()
	totalChunks = contadorChunks
	//indice = 0
	return &proto.Setup{NumChunks: contadorChunks, Titulo: titulo}, nil
}

func (server *server) SubirArchivo(stream proto.ServicioSubida_SubirArchivoServer) (err error) {
	req, err := stream.Recv()
	if err != nil {
		fmt.Printf("Error al recibir informacion sobre el archivo: %v\n", err)
		return nil
	}
	nombre := req.GetNombre()
	for {
		recepcion, e := stream.Recv()
		if e != nil {
			if e == io.EOF {
				goto END
			}

			fmt.Printf("Fallo inesparado al leer archivos desde el stream\n")
			return
		}
		newFileName := nombre
		fmt.Println("Filename:" + newFileName)
		indice++
		//contadorChunks++
		_, err = os.Create(newFileName)

		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		ioutil.WriteFile(newFileName, recepcion.GetContenido(), os.ModeAppend)

	}

END:
	// once the transmission finished, send the
	// confirmation if nothign went wrong

	err = stream.SendAndClose(&proto.StatusSubida{
		Mensaje: "Archivo recibido",
		Codigo:  proto.CodigoStatusSubida_Exitoso,
	})
	contadorChunks--
	if contadorChunks == 0 {
		indice = 0
		//fmt.Println("Going to the other side")
		generarPropuesta(totalChunks)
		condChunk.L.Unlock()
	}
	return

}

func (server *server) DescargarArchivo(request *proto.Solicitud, stream proto.ServicioSubida_DescargarArchivoServer) error {
	archivo := request.GetNombreChunk()
	final := archivo[len(archivo)-1:]
	numero, _ := strconv.Atoi(final)
	//numero--
	fmt.Println(strconv.Itoa(numero))
	letras := strconv.Itoa(numero)
	archivoReal := archivo[:len(archivo)-2] + ".pdf_" + letras
	file, err := os.Open(archivoReal)
	if err != nil {
		log.Fatalf("Error al abrir el archivo: %v", err)
		return err
	}

	defer file.Close()
	buf := make([]byte, chunkSize)
	n, err := file.Read(buf)
	if err != nil {
		log.Fatalf("Error while copying from file to buf:%v", err)
		return err
	}

	req := &proto.Chunk{
		Data: &proto.Chunk_Contenido{
			Contenido: buf[:n],
		},
	}

	err = stream.Send(req)

	if err != nil {
		fmt.Printf("Error al enviar el archivo a través del stream:%v\n", err)
		return err
	}
	fmt.Println("Chunk enviado a cliente")

	return nil

}

func confirmarDistribucion(propuesta *proto.Propuesta)(distribucion *proto.Propuesta){
	var acuerdo = false
	var slice []string
	var dist=propuesta
	for !acuerdo{
		acuerdo=true
		direccionesActivas=checkDireccionesActivas(direcciones[0:2])
		for _,v := range direccionesActivas {
			if v==self{
				//check propuesta propia
				for _, value := range dist.Asignacion{
					if direcciones[value.PosDireccion]==self{
						continue
					}else{
						_,found:=findInSlice(slice,direcciones[value.PosDireccion])
						if !found{
							slice=append(slice,direcciones[value.PosDireccion])
						}
					}
				}
				for _, value:= range slice{
					acuerdo=acuerdo&&isAlive(value)
				}
				slice=slice[:0]
			}else{
				con, err := grpc.Dial(v, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(800*time.Millisecond))
				if err !=nil{
					//log.Fatalf("Failed to connect: %s", err)
					continue
				}
				//llamar funcion gRPC
				client := proto.NewServicioSubidaClient(con)
				reply,err:=client.ConfirmarPropuesta(context.Background(), dist)
				if err!=nil{
					continue
				}
				fmt.Println(reply.GetAceptar())
				acuerdo=acuerdo && reply.GetAceptar()
				fmt.Println(acuerdo)
			}
		}
		if !acuerdo{
			//hacer nueva propuesta
			numChunks:=dist.Nchunks
			direccionesActivas=checkDireccionesActivas(direcciones[0:2])
			fmt.Println(len(direccionesActivas))
			asignaciones := make([]*proto.Asignacion, numChunks)
			//fmt.Println("NumeroChunks:", numChunks)
			pos := int64(-1)
			for i := int64(0); i < numChunks; i++ {
				//dado := rand.Float64()
				asignaciones[i] = new(proto.Asignacion)
				k,_ :=findInSlice( direcciones[0:2],direccionesActivas[(pos + 1) % int64(len(direccionesActivas))])
				asignaciones[i].PosDireccion = int64(k)
				pos++
				asignaciones[i].NumChunk = i + int64(1)
				fmt.Println("Generated proposal(2):" + direcciones[asignaciones[i].PosDireccion])
			}
			dist = &proto.Propuesta{
				Asignacion: asignaciones,
				Titulo:     titulo,
				Nchunks:    numChunks,
			}
			fmt.Println("finalized Generating proposal(2):")
			distribucion=dist
		}
		distribucion=dist
	}
	return distribucion
}

func isAlive(direccion string)(alive bool){
	con, err := grpc.Dial(direccion, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(800*time.Millisecond))
	if err != nil {
		alive=false
		return alive
	}
	con.Close()
	alive = true
	return alive
}

func checkDireccionesActivas(verificar []string)(activas[]string){
	for _,v := range verificar {
		if v==self{
			activas=append(activas,v)
		}else{
			if isAlive(v){
				activas=append(activas,v)
			}
		}
	}
	return activas
}

func findInSlice(slice []string, val string) (int, bool) {
    for i, item := range slice {
        if item == val {
            return i, true
        }
    }
    return -1, false
}

func (server *server) ConfirmarPropuesta(ctx context.Context, propuesta *proto.Propuesta) (*proto.Confirmacion, error){
	direccionesActivas=checkDireccionesActivas(direcciones[0:2])
	var acuerdo =true
	var slice []string
	for _, value := range propuesta.GetAsignacion(){
		if direcciones[value.PosDireccion]==self{
			continue
		}else{
			_,found:=findInSlice(slice,direcciones[value.PosDireccion])
			if !found{
				slice=append(slice,direcciones[value.PosDireccion])
			}
		}
	}
	for _, value:= range slice{
		acuerdo=acuerdo&&isAlive(value)
	}
	fmt.Println(acuerdo)
	response:= proto.Confirmacion{Aceptar:acuerdo}
	return &response, nil
}

func requestCritical(){
	direccionesActivas = checkDireccionesActivas(direcciones[0:2])
	var wg sync.WaitGroup
	clockMutex.Lock()
	clock++
	timestamp:=clock
	criticalClock=clock
	clockMutex.Unlock()
	for _,v := range direccionesActivas{
		if v==self{
			continue
		}
		wg.Add(1)
		go sendRequest(v,timestamp, &wg)
	}
	wg.Wait()
}

func sendRequest(address string, timestamp int64, wg *sync.WaitGroup){
	con, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(800*time.Millisecond))
	if err!=nil{
		log.Fatalf("Failed to connect: %s", err)
	}			
	//llamar funcion gRPC
	client := proto.NewServicioSubidaClient(con)
	request:= proto.Ramessage{
		Clock: timestamp,
		IdDataNode: idDataNode,
	}
	response,err:= client.PedirAccesoCritico(context.Background(), &request)
	contadorMensajes +=int64(2)
	clockMutex.Lock()
	if response.GetClock()>clock{
		clock=response.GetClock()
	}
	clockMutex.Unlock()
	wg.Done()
}

func (server *server) PedirAccesoCritico(ctx context.Context, request *proto.Ramessage) (*proto.Ramessage,error){

	clockMutex.Lock()
	if request.GetClock()>clock{
		clock=request.GetClock()
	}
	clock++
	timestamp:=clock
	criticalTimestamp:=criticalClock
	clockMutex.Unlock()
	var reply proto.Ramessage
	if criticalTimestamp==0 ||request.GetClock()<criticalTimestamp||(criticalTimestamp==request.GetClock()&&request.GetIdDataNode()<idDataNode){
		reply= proto.Ramessage{
			Clock: timestamp,
			IdDataNode: idDataNode,		
		}
	}else{
		criticalMutex.Lock()
		criticalMutex.Unlock()
		reply= proto.Ramessage{
			Clock: timestamp,
			IdDataNode: idDataNode,		
		}
	}
	return &reply,nil
}

func timeTrack(start time.Time, name int) {
	elapsed := time.Since(start)
	log.Printf("El nodo %d tardó %s segundos en completar su solicitud de acceso y escritura al log", name, elapsed)
}
