def filtraParole(l):
	parole = []
	titolo = "".join(ch for ch in l[5].lower() if (ch==" " or ch.isalnum()))
	for w in l[12].split(" ")+l[13].split(" "):
		w = "".join(ch for ch in w.lower() if ch.isalnum())
		if w not in titolo:
			parole.append(((l[7],w),1))
	return parole

def salvasufile(lista_risultato):
	primo = lista_risultato.__next__()
	stars = primo[0][0]
	with open('/home/amircoli/Andrea_Chiorrini/'+stars+'.txt', 'w') as filehandle:
		filehandle.write(str(primo[0][1])+";"+str(primo[1])+"\n")
		for riga in lista_risultato:
			filehandle.write(str(riga[0][1])+";"+str(riga[1])+"\n")

data = sc.textFile("hdfs://192.168.104.45:9000/test.tsv")
datasplit = data.map(lambda s: s.split("\t"))
datafilter = datasplit.filter(lambda l: len(l)==15 and l[11]=="Y")
dataparole = datafilter.flatMap(filtraParole).reduceByKey(lambda a,b:a+b)

dataparole.cache()

dataparoletot = dataparole.map(lambda c: (c[0][1],c[1])).reduceByKey(lambda a,b:a+b)
MAX = dataparoletot.max(lambda x:x[1])[1]
MAX = sc.broadcast((0.05*MAX,0.8*MAX))

dataparolefiltrate = dataparole.filter(lambda x: x[1]>MAX.value[0] and x[1]<MAX.value[1])
datapartition = dataparolefiltrate.partitionBy(5,lambda x: int(x[0][0]))
datapartition.foreachPartition(salvasufile)