import re
import csv
#  from neo4jrestclient.client import GraphDatabase
#  db = GraphDatabase("http://localhost:7474", username="neo4j", password="s")

# Parsed Data
pattern = re.compile(r"(?P<LineNum>\d+)\s"
                     r"<(?P<Components>.*?):"
                     r"(?P<Comp_line>\d+)>\s"
                     r"{(?P<Cycle>\d+)}"
                     r".*?MemoryMessage\[(?P<MemMsg>.*?)\]"
                     r".*?Addr:(?P<Addr>.*?)\s"
                     r".*?Size:(?P<Size>\d+)\s"
                     r".*?Serial:\s(?P<Serial>\d+)\s"
                     r".*?Core:\s(?P<Core>\d+)\s"
                     r".*?DStream:\s(?P<DStream>.*?)\s"
                     r".*?Outstanding Msgs:\s(?P<Msg>.*?)$")

pattern_instr = re.compile(r"(?P<LineNum>\d+)\s"
                           r"<(?P<Components>.*?):"
                           r"(?P<Comp_line>\d+)>\s"
                           r".*?instr=>> #(?P<InstrNum>\d+)"
                           r"\[(?P<CPU_id>\d+)\]\s"
                           r"@PC=\s(?P<PC>.*?)\s"
                           r"opc=\|\s(?P<Opcode>.*?)\s\|\s"
                           r"Disas=(?P<Disas>.*?)"
                           r"{(?P<Semantic>.*?)}\s<<")

class Line:
    def __init__(self, parsed):

        self.LineNum = parsed.group('LineNum')
        self.Components = parsed.group('Components')
        self.Comp_line = parsed.group('Comp_line')
        self.Cycle = parsed.group('Cycle')
        self.MemMsg = parsed.group('MemMsg')
        self.Addr = parsed.group('Addr')
        self.Size = parsed.group('Size')
        self.Serial = parsed.group('Serial')
        self.Core = parsed.group('Core')
        self.DStream = parsed.group('DStream')
        self.Msg = parsed.group('Msg')

    def set_node(self, node):
        self.node = node

    def get_node(self):
        return self.node

    def set_rel(self, type, rel):
        if type == 'Serial':
            self.serial_rel = rel
        elif type == 'Addr':
            self.addr_rel = rel

    def get_rel(self, type):
        if type == 'Serial':
            return self.serial_rel
        elif type == 'Addr':
            return self.addr_rel

class Instr:
    def __init__(self, parsed):
        self.LineNum = parsed.group('LineNum')
        self.Components = parsed.group('Components')
        self.Comp_line = parsed.group('Comp_line')
        self.InstrNum = parsed.group('InstrNum')
        self.CPU_id = parsed.group('CPU_id')
        self.PC = parsed.group('PC')
        self.Opcode = parsed.group('Opcode')
        self.Disas = parsed.group('Disas')
        self.Semantic = parsed.group('Semantic')

    def set_node(self, node):
        self.node = node

    def get_node(self):
        return self.node

class Pattern:
    def __init__(self, pattern_str):
        self.pattern= pattern_str
        self.serials = []   # list of matched serials
        self.addrs = []     # list of matched address
        self.avg_pattern_cycle = []
        self.type = ""

    def add_serial(self, serial):
        self.serials.append(serial)
        self.type = "Serial"

    def add_addr(self, addr):
        self.addrs.append(addr)
        self.type = "Addr"

    def set_node(self, node):
        self.node = node

    def get_node(self):
        return self.node

    def get_type(self):
        return self.type

    def get_average(self):

        if self.type == "Serial":
            for s in self.serials:

                pattern_cycle = []
                prev = dict[s][0]
                for line in dict[s]:
                    pattern_cycle.append(int(line.Cycle) - int(prev.Cycle))
                    prev = line

                if len(self.avg_pattern_cycle) == 0:
                    self.avg_pattern_cycle = pattern_cycle
                else:
                    for i in range(0, len(self.avg_pattern_cycle)):
                        self.avg_pattern_cycle[i] += pattern_cycle[i]

            for i in range(0, len(self.avg_pattern_cycle)):
                self.avg_pattern_cycle[i] /= len(self.serials)

        else:
            for a in self.addrs:

                pattern_cycle = []
                prev = dict[a][0]
                for line in dict[a]:
                    pattern_cycle.append(int(line.Cycle) - int(prev.Cycle))
                    prev = line

                if len(self.avg_pattern_cycle) == 0:
                    self.avg_pattern_cycle = pattern_cycle
                else:
                    for i in range(0, len(self.avg_pattern_cycle)):
                        self.avg_pattern_cycle[i] += pattern_cycle[i]

            for i in range(0, len(self.avg_pattern_cycle)):
                self.avg_pattern_cycle[i] /= len(self.addrs)

        return self.avg_pattern_cycle
lines = {}
dict = {}               # dict[('Serial', '241'), ('Addr', '0xp:0413da82c')] = Line
dict_instr = {}         # dict['instr'] = instr_line
patterns_serial = {}    # patterns[str(Comp:line#)] = Pattern
patterns_addr = {}      # patterns[str(Comp:line#)] = Pattern


if __name__ == "__main__":

    # Parse Data from 'debug.out'
    f = open('/debug.out', 'r')

    #node
    f_serial = open('serial.csv', 'w')
    f_addr = open('addr.csv', 'w')
    f_line = open('line.csv', 'w')
    f_pattern_serial = open('pattern_serial.csv', 'w')
    f_pattern_addr = open('pattern_addr.csv', 'w')
    f_trace = open('trace.csv', 'w')
    f_instr = open('instr.csv', 'w')
    f_instr_line = open('instr_line.csv', 'w')

    #relations
    f_done = open('done.csv', 'w')
    f_match = open('match.csv', 'w')
    f_next_temp = open('next_temp.csv', 'w')
    f_next = open('next.csv', 'w')

    csvWriter_line = csv.writer(f_line)
    csvWriter_line.writerow(['lineId:ID', 'Components', 'Comp_line', 'Cycle', 'MemMsg', 'Addr', 'Size','Serial', 'Core', 'DStream', 'Msg', 'InstrNum', 'CPU_id', 'PC', 'Opcode', 'Disas', 'Semantic'])

    csvWriter_instr_line = csv.writer(f_instr_line)
    csvWriter_instr_line.writerow(['lineId:ID', 'Components', 'Comp_line', 'InstrNum'])

    csvWriter_Serial = csv.writer(f_serial)
    csvWriter_Serial.writerow(['serialId:ID', 'serial'])

    csvWriter_instr = csv.writer(f_instr)
    csvWriter_instr.writerow(['instrId:ID', 'InstrNum', 'CPU_id', 'PC', 'Opcode', 'Disas', 'Semantic'])

    csvWriter_Addr = csv.writer(f_addr)
    csvWriter_Addr.writerow(['addrId:ID', 'addr'])

    csvWriter_done = csv.writer(f_done)
    csvWriter_done.writerow([':START_ID', "lineNum", "hide", ':END_ID'])

    csvWriter_trace = csv.writer(f_trace)
    csvWriter_trace.writerow(['traceId:ID', 'components'])
    traceId = 1000
    
    csvWriter_next_temp = csv.writer(f_next_temp)
    csvWriter_next_temp.writerow([':START_ID', 'cycle', 'comp_line', 'addr', 'serial', 'ID', ':END_ID'])
    rel_ID = 1
    
    csvWriter_next = csv.writer(f_next)

    csvWriter_pattern_serial = csv.writer(f_pattern_serial)
    csvWriter_pattern_serial.writerow(['patternId:ID', 'pattern'])
    pscnt = 1

    csvWriter_pattern_addr = csv.writer(f_pattern_addr)
    csvWriter_pattern_addr.writerow(['patternId:ID', 'pattern'])
    pacnt = 1

    csvWriter_match = csv.writer(f_match)
    csvWriter_match.writerow([':START_ID', 'hide', ':END_ID'])

    print("START - Add Line nodes")
   
    for s in f:
        parsed = pattern.search(s)
        parsed_instr = pattern_instr.search(s)

        if parsed:
            line = Line(parsed)

            if parsed_instr:

                instr = Instr(parsed_instr)
                csvWriter_line.writerow([line.LineNum, line.Components, line.Comp_line, line.Cycle,
                line.MemMsg, line.Addr, line.Size, line.Serial, line.Core, line.DStream, line.Msg, instr.InstrNum, instr.CPU_id, instr.PC, instr.Opcode, instr.Disas, instr.Semantic])
                instr.set_node(line.LineNum)

                if instr.InstrNum not in dict_instr:
                    dict_instr[instr.InstrNum] = [instr]
                else:
                    dict_instr[instr.InstrNum].append(instr)

            else:
                csvWriter_line.writerow([line.LineNum, line.Components, line.Comp_line, line.Cycle,
                line.MemMsg, line.Addr, line.Size, line.Serial, line.Core, line.DStream, line.Msg, None, None, None, None, None, None])

            new = {}
            new = new.fromkeys([("Serial", line.Serial), ("Addr", line.Addr)], line)

            for k, v in new.items():
                if k not in dict:
                    dict[k] = [v, ]
                else:
                    dict[k].append(v)

        elif parsed_instr:
            instr = Instr(parsed_instr)
            csvWriter_instr_line.writerow(["il"+str(instr.LineNum), instr.Components, instr.Comp_line, instr.InstrNum])
            instr.set_node("il"+str(instr.LineNum))
            if instr.InstrNum not in dict_instr:
                dict_instr[instr.InstrNum] = [instr]
            else:
                dict_instr[instr.InstrNum].append(instr)

    f_line.close()
    print("END - Add Line nodes\n")

    print("START - Categorize lines")
    for k, v in dict.items():
        
        parent = ""
        if k[0] == 'Serial':
            parent = 's'+k[1]
        
        elif k[0] == 'Addr':
            parent = 'a'+k[1]

        current_trace = {}

        if k[0] == 'Serial':
            csvWriter_Serial.writerow(['s'+k[1], k[1]])
            current_trace["start"] = 's'+k[1]
        
        elif k[0] == 'Addr':
            csvWriter_Addr.writerow(['a'+k[1], k[1]])
            current_trace["start"] = 'a'+k[1]

        last = "start"
        last_cycle = v[0].Cycle

        pattern =()

        i = 0
        for line in v:

            i += 1
            
            if k[0] == 'Serial':
                csvWriter_done.writerow(['s'+k[1], line.LineNum, " ", line.LineNum])
            elif k[0] == 'Addr':
                csvWriter_done.writerow(['a'+k[1], line.LineNum, " ", line.LineNum])

            if line.Components not in current_trace:
                csvWriter_trace.writerow(['t'+str(traceId), line.Components])
                current_trace[line.Components] = 't'+str(traceId)
                
                cycle_str = str(i) + str(" (") + str(int(line.Cycle) - int(last_cycle)) + str(")")
                if line == v[-1]:
                    cycle_str += " Fin."
                
                csvWriter_next_temp.writerow([current_trace[last], cycle_str, line.Comp_line,
                    line.Addr,line.Serial, 'rel'+str(rel_ID), 't'+str(traceId)])
                
                line.set_rel(k[0], 'rel'+str(rel_ID))
                last = line.Components
                last_cycle=line.Cycle
                traceId += 1
                rel_ID += 1
            
            else:
                cycle_str = str(i) + str(" (") + str(int(line.Cycle) - int(last_cycle)) + str(")")
                if line == v[-1]:
                    cycle_str += " Fin."

                csvWriter_next_temp.writerow([current_trace[last], cycle_str, line.Comp_line,
                    line.Addr,line.Serial, 'rel'+str(rel_ID), current_trace[line.Components]])
                
                line.set_rel(k[0], 'rel'+str(rel_ID))
                last = line.Components
                last_cycle = line.Cycle
                rel_ID += 1

            p = str(line.Components) + "/" + str(line.Comp_line)
            pattern = pattern + (p, )


        if k[0] == 'Serial':
            if pattern in patterns_serial:
            
                patterns_serial[pattern].add_serial(k)
                pattern_node = patterns_serial[pattern].get_node()
                csvWriter_match.writerow([pattern_node, " ", parent]) 

            else:
                p = Pattern(pattern)
                p.add_serial(k)
                patterns_serial[pattern] = p
                
                csvWriter_pattern_serial.writerow(['P:S:'+str(pscnt), pattern])        
                p.set_node('P:S:'+str(pscnt))
                
                csvWriter_match.writerow(['P:S:'+str(pscnt), " ", parent])

                pscnt+=1

        elif k[0] == 'Addr':
            if pattern in patterns_addr:
                patterns_addr[pattern].add_addr(k)
                pattern_node = patterns_addr[pattern].get_node()
                csvWriter_match.writerow([pattern_node, " ", parent]) 

            else:
                p = Pattern(pattern)
                p.add_addr(k)
                patterns_addr[pattern] = p
                
                csvWriter_pattern_addr.writerow(['P:A:'+str(pacnt), pattern])        
                p.set_node('P:A:'+str(pacnt))
                
                csvWriter_match.writerow(['P:A:'+str(pacnt), " ", parent])

                pacnt+=1

    print("END - Categorize lines\n")

    print("START - Categorize instr")

    for k, v in dict_instr.items():
        csvWriter_instr.writerow(['i'+str(k), k, v[0].CPU_id, v[0].PC, v[0].Opcode, v[0].Disas, v[0].Semantic])

        for line in v:
            csvWriter_done.writerow(['i'+str(k), line.LineNum, " ", line.get_node()])

    print("END - Categorize instr\n")


    print("START - avg_cycle for serial calculation")
    
    save_avg = {}

    for pattern_str, Ptn in patterns_serial.items():

        avg_cycle = Ptn.get_average()
        
        for serial in Ptn.serials:
            for i in range(0, len(dict[serial])):
                save_avg[dict[serial][i].get_rel('Serial')] = avg_cycle[i]

    print("END - avg_serial calculation\n")

    print("START - avg_cycle for addr calculation")
    for pattern_str, Ptn in patterns_addr.items():

        avg_cycle = Ptn.get_average()
        for addr in Ptn.addrs:
            for i in range(0, len(dict[addr])):
                save_avg[dict[addr][i].get_rel('Addr')] = avg_cycle[i]
    print("END - avg_cycle for addr calculation\n")

    f_next_temp.close()
    
    f_next_temp = open('next_temp.csv', 'r')
    csvReader = csv.reader(f_next_temp)

    all = []
    row = next(csvReader)
    row.append('avg_cycle')
    all.append(row)

    for row in csvReader:
        if row[5] in save_avg:
            row.append(save_avg[row[5]])
        else:
            row.append(0.0)
        all.append(row)

    csvWriter_next.writerows(all)
