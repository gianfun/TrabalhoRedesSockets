import socket
import traceback
import multiprocessing as mp
import sys

#Contem as contantes!
import ConstantesRedes

"""
    Este servidor recebe de um cliente, pela rede, um arquivo, que é salvo.
    Para tal, utiliza 2 processos:
        escritorArquivo - Roda num processo só. Lê a fila de blocos dos
                        processos receptores de dados e escreve os blocos,
                        cada um com tamanho ConstantesRedes.BLOCKSIZE, para
                        um arquivo.
        
        receiverSocket  - Roda em N processos, onde N é definido em
                        ConstantesRedes.NUMRECEPTOR. Cada processo
                        recebe blocos pelo socket e os coloca numa fila.
    Existem 2 modos de funcionamento:
         MULTITCP = True - Recebe por N conexões diferentes.
         MULTITCP = False - Recebe por 1 conexão compartilhada.

         Para o caso de MULTITCP = True, ConstantesRedes.NUMTRANSMISSOR
         deve ser igual a ConstantesRedes.NUMRECEPTOR
"""

def escritorArquivo(buffer, filename):
    """
        Recebe na fila 'buffer' os blocos (de tamanho ConstantesRedes.BLOCKSIZE), e
        os grava no arquivo 'filename'. Cada bloco tem um número de sequência.
        Caso os blocos cheguem fora de ordem, eles são gravados num buffer
        secundario e esperá-se todos os blocos anteriores a ele chegarem.
    """
    #Como estamos num processo diferentes, vamos dar prints e mandar
    #exceções para arquivos (pois não conseguimos imprimir para o terminal
    #de maneira segura (Devido a serem varios processos).
    sys.stdout = open("logEscritorArquivo.txt", "w")
    sys.stderr = open("logEscritorArquivo_error.txt", "w")

    #Tentamos abrir o arquivo para escrita (este é o arquivo recebido)
    try:
        arq = open(filename, "wb")
    except (FileNotFoundError):
        print("Arquivo não pode ser aberto")
        return

    print("Consegui abrir\n")
    sys.stdout.flush()

    #Precisamos contar quantos processos transmissores (do cliente) ou receptores acabaram,
    #para sabermos quando os blocos acabaram.
    stopCounter = ConstantesRedes.NUMTRANSMISSOR
    continua = True
    blocoAtual = 1
    blocosEspera = {}
    blocosEsperaIdx = []

    #Enquanto pudermos continuar e o buffer não estiver vazio
    while continua or not buffer.empty():
        print("Bloco esperado:"+str(blocoAtual))
        sys.stdout.flush()

        #Pegamos dados do buffer
        dados = buffer.get()
        
        print("Bloco recebido:"+str(dados[0]))
        print(str(dados[1]) + "\n")
        sys.stdout.flush()

        if dados[0] == -1:
            #Se o dado tiver -1, sabemos que é uma mensagem que um processo de
            #recepção se fechou (pois acabaram os dados)
            print("Uma thread de recepcao se fechou")
            stopCounter -= 1    #Decrementamos o contador de parada
            print("Faltam %d"%stopCounter)
            sys.stdout.flush()
            if stopCounter == 0: #Caso tenha sido o último processo de recepção
                print("StopCounter em 0. Terminando...")
                sys.stdout.flush()
                continua = False #Podemos parar de receber.
        else: #Se forem dados do arquivo
            if blocoAtual == dados[0]: #Se o bloco recebido for o próximo, sequencialmente
                print("Escrevendo..")
                sys.stdout.flush()

                #Escrevemos no arquivo
                arq.write(dados[1])
                blocoAtual += 1 #Incrementamos o id do bloco que estamos esperando
                print("Checando blocos no buffer")
                #E agora procuramos no buffer secundario se os blocos chegaram
                while blocoAtual in blocosEsperaIdx:
                    #Enquanto o bloco esperado estiver no buffer secundario
                    print("Bloco %d ja tinha chegado. Escrevendo-o..."%(blocoAtual))
                    sys.stdout.flush()

                    #Remover ele do buffer
                    blocosEsperaIdx.remove(blocoAtual)
                    #Escrever no arquivo
                    arq.write(blocosEspera[blocoAtual])
                    #Remover os dados dele do buffer
                    del(blocosEspera[blocoAtual])
                    #Incrementar o id do bloco esperado
                    blocoAtual += 1
                print("Ok.")    
            else: #Se o bloco recebido não for o próximo sequencialmente
                #Gravá-lo no buffer secundário.
                blocosEsperaIdx.append(dados[0])
                blocosEspera[dados[0]] = dados[1]

        
    #Não existem processos receptores aberto, e a fila esta vazia. Podemos terminar este processo.
    print("Terminando... \n")
    #Fechamos os arquivos
    arq.close()
    sys.stdout.close()
    sys.stderr.close()
        
def receiverSocket(buffer, sock, idx):
    """
        Função que recebe pelo socket 'sock' blocos do arquivo do cliente.
        Estes são colocados na fila 'buffer' e enviados ao processo escritor
        de arquivo.
    """

    #Como estamos num processo diferentes, vamos dar prints e mandar
    #exceções para arquivos (pois não conseguimos imprimir para o terminal
    #de maneira segura (Devido a serem varios processos).
    sys.stdout = open("logServerReceiver%d.txt"%idx, "w")
    sys.stderr = open("logServerReceiver_error%d.txt"%idx, "w")

    #Caso receba varias mensagens de término, precisamos contá-las
    cntFinal = 0
    
    print("%d: Começando loop receiverSocket\n"%idx)
    continua = True
    while continua: #Enquanto o flag for verdadeiro
        print("%d: Recebendo..."%idx)
        sys.stdout.flush()
        
        try:
            print("A")
            sys.stdout.flush()

            #lemos dados do socket
            dados = sock.recv(ConstantesRedes.BLOCKSIZE + 9)

            #Se for uma mensagem de "Ola" (quando processo transmissor é iniciado"
            if dados[0:9] == b"-00000020":
                #Podemos ignorar
                print("%d: Recebi Ola! Ignorando..."%idx)
                print("%d: Ignored: (%d)\n%s"%(idx, len(dados),dados.decode("utf-8")))
                sys.stdout.flush()
                #E lemos alguns dados a mais (Para que o bloco tenha o tamanho esperado)
                newDados = sock.recv(ConstantesRedes.BLOCKSIZE + 9 - (len(dados)-9))
                dados = dados[9:] + newDados
                
            print("%d: OK! (%d)\n%s"%(idx, len(dados),dados.decode("utf-8")))
            sys.stdout.flush()
            #Pegamos o numero de sequencia do bloco
            nSeq = int(dados[0:8], 16)
            #E vemos se é um bloco final (Ele será menor que o esperado)
            menor = int(dados[8:9])
            print("NSEG: %d\tmenor:%d"%(nSeq, menor))
            print("B")
            sys.stdout.flush()
        except socket.timeout:
            #Se ocorrer timeout, saimos.
            print("\nConexão fechada\n")
            sys.stdout.flush()
            buffer.put((-1, ""))
            dados = None
        finally:
            print("C")
            sys.stdout.flush()


        print("\n%d: Pondo no Buffer..."%idx)
        sys.stdout.flush()
        if dados != None: #Se não ocorreu exceção

            #Se for um bloco final
            if menor == 1:
                print('dados[-8:] == b"-0000001"? '+str(dados[-8:] == b"-0000001"))
                #Removemos o final do bloco (Que conterá mensagem de 'acabou o bloco')
                while dados[-9:] == b"-00000011":
                    print("Recebi um -1 no final")
                    sys.stdout.flush()
                    dados = dados[:-9]
                    cntFinal += 1
                #E podemos parar.
                continua = False
                print("%d: Era ultimo bloco"%idx)
                sys.stdout.flush()
            #Colocamos o bloco no buffer
            buffer.put((nSeq, dados[9:]))
        print("OK!...")
        if nSeq == -1: #Se o número de sequencia for -1, acabou a transmissão
            continua = False
            
    #Avisamos o escritor que estamos fechando.
    for i in range(cntFinal):
        buffer.put((-1, ""))
    print("%d: Acabei recepcao.!\n"%idx)
    sys.stdout.close()
    sys.stderr.close()
    print("Tchau")
    

if __name__ == '__main__':
    #Queremos gerar um processo novo
    mp.set_start_method("spawn")
    #Criamos uma fila para os blocos do arquivo
    blocosArquivo = mp.Queue()

    #Caso seja rodado no terminal, pegamos o nome do arquivo
    #que será enviado
    filename = sys.argv[1:]
    filename = "recebido.txt"
    if len(filename) == 0:
        filename = input("Qual o arquivo a ser recebido? ")
    print ("Arquivoo recebido terá nome: '%s'"%filename)

    #criamos um socket
    sock = socket.socket()
    try:
        #Tentamos ligá-lo à porta e endereço.
        print("Socket criado")
        sock.bind((ConstantesRedes.ADDR, ConstantesRedes.PORT))
        print("Socket fez Bind em (",ConstantesRedes.ADDR,",", ConstantesRedes.PORT,")")
    except OSError:
        sock = None
        print("Ocorreu um erro durante o Bind()")
        traceback.print_exc()

    #Se não ocorreu erro
    if sock != None:
        sock.listen(1)
        print("Socket na escuta")

        
        continua = True
        sockets = []
            
        if ConstantesRedes.MULTITCP:    #Se formos utilizar varias conexões
            if ConstantesRedes.NUMTRANSMISSOR != ConstantesRedes.NUMRECEPTOR:
                #Checamos se podemos.
                print("Não posso receber multiplas conexões com NUMTRANSMISSOR != NUMRECEPTOR")
                continua = False
            else:
                #Para cada receptor, aceitamos uma conexão
                for i in range(ConstantesRedes.NUMRECEPTOR):
                    s = sock.accept()
                    print(s[1], "se conectou")
                    #E colocamos na lista de conexões abertas
                    sockets.append(s)
        else:                           #Se formos compartilhar uma conexão
            s = sock.accept()
            print(s[1], "se conectou")
            sockets = [s for i in range(ConstantesRedes.NUMRECEPTOR)]

        #Se tudo estiver ok
        if continua:
                    
            print("Comecando processos")
            #Criamos um processo escritor de arquivos
            escritor = mp.Process(target = escritorArquivo, args=(blocosArquivo, filename))

            #Criamos ConstantesRedes.NUMRECEPTOR processos receptores
            receptores = []
            for i in range(ConstantesRedes.NUMRECEPTOR):
                receptor = mp.Process(target = receiverSocket, args=(blocosArquivo, sockets[i][0], i))
                #E colocamos eles nuam lista
                receptores.append(receptor)

            #Iniciamos o processo escritor
            escritor.start()
            print("Comecando receptores")
            #Iniciamos cada processo receptor
            for i in range(ConstantesRedes.NUMRECEPTOR):
                receptores[i].start()
            print("Dando join nos receptores")

            #Esperamos cada processo receptor terminar
            for i in range(ConstantesRedes.NUMRECEPTOR):
                print("Dando join no #%d"%i)
                receptores[i].join()
                print("Acabou")

            #E esperamos o processo escritor terminar
            print("Dando join no escritor")
            escritor.join()

            #Fechamos o socket (só existe um, com varias conexões)
            sock.close()

            #Avisamos o usuário que terminamos
            print("Terminei.")
        
