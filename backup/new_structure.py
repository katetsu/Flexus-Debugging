import re
from neo4jrestclient.client import GraphDatabase
db = GraphDatabase("http://localhost:7474", username="neo4j", password="s")

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

lines = []  # Base Node. List of Line
dict = {}   # dict[('Serial', '241'), ('Addr', '0xp:0413da82c')] = Line
patterns_serial = {}   # patterns[str(Comp:line#)] = Pattern
patterns_addr = {}   # patterns[str(Comp:line#)] = Pattern

if __name__ == "__main__":

    # Parse Data from 'debug.out'
    f = open('debug.out', 'r')

    # Let's add nodes to net4j Database!!
    node_serial = db.labels.create("Serial")
    node_addr = db.labels.create("Addr")
    node_line = db.labels.create("Line")
    node_pattern_serial = db.labels.create("Pattern_serial")
    node_pattern_addr = db.labels.create("Pattern_addr")
    node_trace = db.labels.create("Trace")

    print("START - Add Line nodes")
    for s in f:
        parsed = pattern.search(s)

        if parsed:
            line = Line(parsed)
            lines.append(line)

            #  l1 = db.nodes.create(Components = line.Components,
            #                       Comp_line = line.Comp_line,
            #                       Cycle = line.Cycle,
            #                       MemMsg = line.MemMsg,
            #                       Addr = line.Addr,
            #                       Size = line.Size,
            #                       Serial = line.Serial,
            #                       Core = line.Core,
            #                       DStream = line.DStream,
            #                       Msg = line.Msg)
            #  l1 = db.nodes.create()
            #  node_line.add(l1)
            #  line.set_node(l1)

            new = {}
            new = new.fromkeys([("Serial", line.Serial), ("Addr", line.Addr)], line)

            for k, v in new.items():
                if k not in dict:
                    dict[k] = [v, ]
                else:
                    dict[k].append(v)
    print("END - Add Line nodes\n")

    print("START - Categorize lines")
    for k, v in dict.items():

        parent = db.nodes.create()
        if k[0] == 'Serial':
            parent.set('Serial', k[1])
            node_serial.add(parent)

        elif k[0] == 'Addr':
            parent.set('Addr', k[1])
            node_addr.add(parent)

        current_trace = {}
        current_trace["start"] = parent
        last = "start"
        last_cycle = v[0].Cycle

        pattern =()

        i = 0
        for line in v:

            i += 1

            parent.relationships.create("done", line.get_node(), at=line.LineNum, hide="")

            if line.Components not in current_trace:
                t1 = db.nodes.create(Components = line.Components)
                current_trace[line.Components] = t1
                node_trace.add(t1)
                cycle_str = str(i) + str(" (") + str(int(line.Cycle) - int(last_cycle)) + str(")")
                if line == v[-1]:
                    cycle_str += " Fin."
                rel = current_trace[last].relationships.create("next", t1, cycle=cycle_str, Comp_line=line.Comp_line, Addr=line.Addr, Serial=line.Serial)
                line.set_rel(k[0], rel)
                last = line.Components
                last_cycle=line.Cycle

            else:
                cycle_str = str(i) + str(" (") + str(int(line.Cycle) - int(last_cycle)) + str(")")
                if line == v[-1]:
                    cycle_str += " Fin."
                rel = current_trace[last].relationships.create("next", current_trace[line.Components], cycle=cycle_str, Comp_line=line.Comp_line, Addr=line.Addr, Serial=line.Serial)
                line.set_rel(k[0], rel)
                last = line.Components
                last_cycle = line.Cycle




            p = str(line.Components) + "/" + str(line.Comp_line)
            pattern = pattern + (p, )


        if k[0] == 'Serial':
            if pattern in patterns_serial:
                patterns_serial[pattern].add_serial(k)
                pattern_node = patterns_serial[pattern].get_node()
                pattern_node.relationships.create("matches", parent, hide="")

            else:
                p = Pattern(pattern)
                pattern_node = db.nodes.create(Pattern = pattern)
                p.add_serial(k)
                node_pattern_serial.add(pattern_node)
                patterns_serial[pattern] = p
                p.set_node(pattern_node)
                pattern_node.relationships.create("matches", parent, hide="")

        elif k[0] == 'Addr':
            if pattern in patterns_addr:
                patterns_addr[pattern].add_addr(k)
                pattern_node = patterns_addr[pattern].get_node()
                pattern_node.relationships.create("matches", parent, hide="")

            else:
                p = Pattern(pattern)
                pattern_node = db.nodes.create(Pattern = pattern)
                p.add_addr(k)
                node_pattern_addr.add(pattern_node)
                patterns_addr[pattern] = p
                p.set_node(pattern_node)
                pattern_node.relationships.create("matches", parent, hide="")
    print("END - Categorize lines\n")

    print("START - avg_cycle for serial calculation")
    cnt = 0
    for pattern_str, Ptn in patterns_serial.items():
        cnt += 1
        pattern_name = "P:S:"+ str(cnt) + str("/")+str(len(patterns_serial))
        Ptn.get_node().set("name", pattern_name)

        avg_cycle = Ptn.get_average()
        for serial in Ptn.serials:
            for i in range(0, len(dict[serial])):
                dict[serial][i].get_rel("Serial").set('avg_cycle', avg_cycle[i])
    print("END - avg_serial calculation\n")

    cnt = 0
    print("START - avg_cycle for addr calculation")
    for pattern_str, Ptn in patterns_addr.items():
        cnt += 1
        pattern_name = "P:A:"+str(cnt)+str("/")+str(len(patterns_addr))
        Ptn.get_node().set("name", pattern_name)

        avg_cycle = Ptn.get_average()
        for addr in Ptn.addrs:
            for i in range(0, len(dict[addr])):
                dict[addr][i].get_rel("Addr").set('avg_cycle', avg_cycle[i])
    print("END - avg_cycle for addr calculation\n")
