import socket
import multiprocessing as mp
import sys

#Contem as contantes!
import ConstantesRedes

"""
    Este cliente lê um arquivo e envia pela rede.
    Para tal, utiliza 2 processos:
        leitorArquivo - Roda num processo só. Lê o arquivo em arquivos
                        de tamanho ConstantesRedes.BLOCKSIZE e coloca
                        em N filas, onde N é o numero de transmissores
        
        senderSocket  - Roda em N processos, onde N é definido em
                        ConstantesRedes.NUMTRANSMISSOR. Cada processo
                        contem sua própria fila de blocos.
                        Pega blocos de sua fila e os envia pelo socket
                        que lhe foi dado.
    Existem 2 modos de funcionamento:
         MULTITCP = True - Envia por N conexões diferentes.
         MULTITCP = False - Envia por 1 conexão compartilhada.

         Para o caso de MULTITCP = True, ConstantesRedes.NUMTRANSMISSOR
         deve ser igual a ConstantesRedes.NUMRECEPTOR
"""

def leitorArquivo(buffers, filename, blocksize):
    """
        Função que o processo leitor roda. Ela lê um arquivo
        de nome filename (no modo binario) e envia para
        buffer (fila) em buffers, alternadamente.
        O tamanho do bloco lido e colocado numa fila é
        blocksize
    """

    #Como estamos num processo diferentes, vamos dar prints e mandar
    #exceções para arquivos (pois não conseguimos imprimir para o terminal
    #de maneira segura (Devido a serem varios processos).
    sys.stdout = open("logLeitorArquivo.txt", "w")
    sys.stderr = open("logLeitorArquivo_error.txt", "w")

    #Tentamos abrir o arquivo para leitura (este é o que será enviado)
    try:
        arq = open(filename, "rb")
    except (FileNotFoundError):
        #Caso ocorra um erro, damos um erro e saimos.
        print("Arquivo não pode ser aberto")
        sys.stdout.flush()
        return

    print("Consegui abrir o arquivo '%s'\n"%filename)
    sys.stdout.flush()
    bufferidx = 0
    blockcnt = 1
    continua = True

    #Enquanto o flag for verdadeiro
    while continua:
        #Lemos um bloco do arquivo
        dados = arq.read(blocksize);
        
        if dados == b'':        #Se chegamos no final do arquivo
            continua = False    #Setamos a flag para False para sair do loop
        else:
            #Se não chegamos no final do arquivo

            #Escrever para um buffer o indice do bloco e os dados do bloco
            buffers[bufferidx % ConstantesRedes.NUMTRANSMISSOR].put((blockcnt, dados))
            bufferidx += 1 #E incrementamos para o próximo buffer

            #Imprimimos para o log
            print("dados: " + str((blockcnt, dados.decode('utf-8'))) + "\n")
            sys.stdout.flush()

            #Vamos para o proximo bloco
            blockcnt += 1

    #Após terminar a leitura, avisamos os processos de envio por socket
    #que terminamos (colocando (-1, "") em suas filas)
    for i in range(ConstantesRedes.NUMTRANSMISSOR):
        buffers[i].put((-1, ""))

    #E informamos, no log, que terminamos
    print("Terminando... \n")
    sys.stdout.flush()

    #Fechamos o arquivo
    arq.close()
    #E os streams de saida do programa 
    sys.stdout.close()
    sys.stderr.close()

def senderSocket(buffer, sock, i):
    """
        Função que o processo enviador roda. Através da
        fila conectadas ao processo leitor de arquivo (o 'buffer'),
        este processo envia blocos para o socket 'sock'.
    """

    #Como estamos num processo diferentes, vamos dar prints e mandar
    #exceções para arquivos (pois não conseguimos imprimir para o terminal
    #de maneira segura (Devido a serem varios processos).
    sys.stdout = open("logSenderSocket%d.txt"%i, "w")
    sys.stderr = open("logSenderSocket_error%d.txt"%i, "w")
    
    print("%d: Começando loop SenderSocket\n"%i)
    sys.stdout.flush()

    #Enviamos ao servidor uma mensagem informando que vamos começar o envio.
    if ConstantesRedes.MULTITCP:
        sock.send(bytes("%08x%1d"%(-2, 0), "utf-8"))

    
    continua = True
    while continua: #Enquanto a flag for verdadeira
        #Pegamos um bloco
        bloco = buffer.get()
        
        if bloco[0] == -1:
            #Se o bloco tiver -1, quer dizer que o leitor terminou de ler o arquivo
            #Portanto, podemos parar de enviar.
            continua = False
        else: #Senão, podemos continuar enviando
            print("%d: Enviando..."%i)

            #Checamos se o bloco é menor que BLOCKSIZE.
            #Se for, quer dizer que chegamos no final do arquivo!
            #Precisamos avisar o servidor que o bloco é menor.
            menor = 0
            if len(bloco[1]) < ConstantesRedes.BLOCKSIZE:
                menor = 1

            #Geramos os dados que serão enviados.
            #Os dados tem o formato:
            #   8 caracteres para informar o número do bloco
            #   1 caracter para informar se é o bloco final.
            #   ConstantesRedes.BLOCKSIZE caracteres com dados.
            enviado = bytes("%08x%1d"%(bloco[0], menor), 'utf-8') + bloco[1]
            print(enviado)
            sys.stdout.flush()

            #Enviamos os dados.
            sock.send(enviado)

            #E informamos, no log, que terminamos de enviar este bloco.
            print("%d: OK!\n"%i)
            sys.stdout.flush()

    #Enviamos ao servidor mensagem de 'acabei de enviar'
    if ConstantesRedes.MULTITCP:
        sock.send(bytes("%08x%d"%(-1, 1), "utf-8"))        

    #Fechamos arquivos e logs
    print("%d: Acabei envio.!\n"%i)
    sys.stdout.flush()
    sys.stdout.close()
    sys.stderr.close()

if __name__ == '__main__':
    #Queremos gerar um processo novo
    mp.set_start_method("spawn")
    
    #Caso seja rodado no terminal, pegamos o nome do arquivo
    #que será enviado
    filename = sys.argv[1:]
    filename = "small.txt"
    filename = "odyssey.txt"
    if len(filename) == 0:
        filename = input("Qual o arquivo a ser transferido? ")
    print (filename)

    #Criaremos os sockets.
    sockets = []
    if ConstantesRedes.MULTITCP:    #Se formos usar varias conexões
        #Criamos uma conexão por transmissor
        for sockCnt in range(ConstantesRedes.NUMTRANSMISSOR):
            #Cria socket
            sock = socket.socket()
            try:
                #Tentamos conectar no servidor
                sock.connect((ConstantesRedes.ADDR, ConstantesRedes.PORT))
                print("Conectado %d."%sockCnt)
            except ConnectionRefusedError:
                #Falhou a conexão
                print("Não foi possivel abrir a conexão! O servidor está rodando?")
                sock = None
            #Colocamos os sockets na lista de sockets conectados.
            sockets.append(sock)
    else:                           #Se formos compartilhar uma conexão
        #Criamos a conexão
        sock = socket.socket()
        try:
            #Tentamos conectar ao servidor
            sock.connect((ConstantesRedes.ADDR, ConstantesRedes.PORT))
            print("Conectado.")
        except ConnectionRefusedError:
            #Falhou a conexão
            print("Não foi possivel abrir a conexão! O servidor está rodando?")
            sock = None
        #Criamos uma lista com este um socket repetidamente (para enviar
        #para todos os NUMTRANSMISSOR transmissores).
        sockets = [sock for i in range(ConstantesRedes.NUMTRANSMISSOR)]

    #Se conseguimos conectar.
    if sock != None:
        print("Comecando processos")
        ##arq = open("")

        senders = []
        buffers = []

        #criamos o processo leitor de arquivos
        leitor = mp.Process(target = leitorArquivo, args=(buffers, filename, ConstantesRedes.BLOCKSIZE))
        #criamos ConstantesRedes.NUMTRANSMISSOR processos transmissores
        for i in range(ConstantesRedes.NUMTRANSMISSOR):
            #Criamos uma fila para o processo
            buffer = mp.Queue()
            #Colocamos na lista de filas
            buffers.append(buffer)
            #Criamos processo
            sender = mp.Process(target = senderSocket, args=(buffer, sockets[i], i))
            #Colocamos na lista de processos de envio
            senders.append(sender)
            
        print("Processos criados")
        print("Comecando Leitor")
        #Iniciamos o processo leitor de arquivos    
        leitor.start()

        print("Comecando Sender")
        #Para cada processo enviador
        for sender in senders:
            #Iniciamos o processo
            sender.start()

        print("Join no Leitor")
        #Esperamos o leitor terminar
        leitor.join()
        print("Leitor terminou")
        #E para cara enviador
        for i in range(ConstantesRedes.NUMTRANSMISSOR):
            print("Join no sender #%d"%i)
            #Esperamos ele terminar também.
            senders[i].join()
            print("Sender terminou")

        #Se todos terminaram, enviar ao servidor:
        print("Enviando Tchau")
        if not ConstantesRedes.MULTITCP:
            for i in range(ConstantesRedes.NUMTRANSMISSOR):
                sock.send(bytes("%08x%d"%(-1, 1), "utf-8"))
            
        #Fechamos os sockets
        print("Fechando sockets")
        if ConstantesRedes.MULTITCP:
            for sock in sockets:
                sock.close()
        else:
            sockets[0].close()

        #Por fim, avisamos o usuários que terminamos.
        print("Terminei.")

