import re
import curses

EXIT = 0
VISUAL_MODE = 1
CNT_UNIQUE = 2
PRINT_ALL = 3
PRINT_SERIAL = 4
CATEGORIZE = 5

# For VISUAL MODE
VISUAL_MODE_ALL = False
WINDOW_SIZE = 70
parsed_data = {}    # {Serial: [group, group, ...]}
patterns = {}


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


def print_all():

    for key in parsed_data.keys():
        print_serial(key)


def print_serial(serial_num):

    print("\n\nSerial: ", serial_num)

    for line in parsed_data[serial_num]:
        print("==============================================")
        #print("LineNum:          ".ljust(20), line.group('LineNum'))
        print("Components:       ".ljust(20), line.group('Components'))
        print("Comp_line:        ".ljust(20), line.group('Comp_line'))
        #print("Cycle:            ".ljust(20), line.group('Cycle'))
        print("MemMsg:           ".ljust(20), line.group('MemMsg'))
        print("Addr:             ".ljust(20), line.group('Addr'))
        #print("Size:             ".ljust(20), line.group('Size'))
        #print("Serial:           ".ljust(20), line.group('Serial'))
        #print("Core:             ".ljust(20), line.group('Core'))
        #print("DStream:          ".ljust(20), line.group('DStream'))
        #print("Outstanding Msgs: ".ljust(20), line.group('Msg'))

def visual_print_serial(stdscr, serial_num):

    stdscr.addstr(1, 1, "Serial: "+str(serial_num))

    printed = {}

    row_range = [(4, 4)]

    cur_win = 0
    cur_row = 4
    cur_col = 0
    next_win = 1

    for line in parsed_data[serial_num]:

        if line.group('Components') in printed:
            #Already Exist Component

            row = printed[line.group('Components')][0]
            col = printed[line.group('Components')][1]
            cnt = printed[line.group('Components')][2]


            if row == cur_row and col == cur_col:
                printed[line.group('Components')] = (row, col, cnt)

                cnt_str = "     "+str(cnt+1) + "     "
                #stdscr.addstr(row+1, 5, cnt_str)#

            elif row > cur_row:
                #Down
                if not VISUAL_MODE_ALL:
                    c = stdscr.getch()
                for r in range(cur_row+2, row-2, 1):
                    stdscr.addstr(r, cur_col+20+cnt*6,   "  ||")
                stdscr.addstr(int((cur_row+row)/2), cur_col+20+cnt*6, line.group('Cycle'))
                stdscr.addstr(row-2, cur_col+20+cnt*6,   "  VV")
                printed[line.group('Components')] = (row, col, cnt+1)
                cur_row = row

            else:
                # Up
                if not VISUAL_MODE_ALL:
                    c = stdscr.getch()
                stdscr.addstr(row+1, cur_col+52, "<--")
                stdscr.addstr(cur_row-1, cur_col+52, "---")

                for r in range(cur_row-2, row+1, -1):
                    stdscr.addstr(r, cur_col+52, "  |  ")

                stdscr.addstr(int((cur_row+row)/2)   , cur_col+52, str(line.group('Cycle')))

                cur_row = row

                if cur_row < row_range[cur_win][0]:
                    #Move Left
                    cur_win -= 1
                    cur_col = cur_win*70

        else:
            #New Component

            cur_row += 6
            if not VISUAL_MODE_ALL:
                    c = stdscr.getch()

            if cur_row <= row_range[cur_win][1]:
                #Move Right
                cur_win += next_win
                next_win += 1
                cur_col = cur_win*70

                row_range.append((cur_row, cur_row))

            stdscr.addstr(cur_row-4, cur_col+20,   "  |")
            stdscr.addstr(cur_row-3, cur_col+20,   line.group('Cycle'))
            stdscr.addstr(cur_row-2, cur_col+20,   "  V")

            stdscr.addstr(cur_row-1, cur_col,   "==================================================")
            stdscr.addstr(cur_row, cur_col,   "|                                                |")
            stdscr.addstr(cur_row+1, cur_col,   "==================================================")

            stdscr.addstr(cur_row, cur_col+2, line.group('Components'))

            printed[line.group('Components')] = (cur_row, cur_col, 1)

            max_row = row_range[cur_win][1]
            if cur_row > max_row:
                row_range[cur_win] = (row_range[cur_win][0], cur_row)


def categorize():

    print("\n    1. Show all\n    2. Categorize with ADDRESS\n    3. Categorize with OPERATION\n\n")

    patterns.clear()
    com = int(input("> "))

    for serial in parsed_data.keys():

        pattern =()
        for line in parsed_data[serial]:
            p = str(line.group('Components')) + "/" + str(line.group('Comp_line'))
            pattern = pattern + (p, )

        if pattern in patterns:
            patterns[pattern].append(serial)
        else:
            patterns[pattern] = [serial, ]

    if com == 1:
        p_cnt = 1
        for pattern in patterns.keys():

            print("\n**************** PATTERN", p_cnt, "****************")
            print(pattern)
            p_cnt += 1

            s_cnt = 0
            for serial in patterns[pattern]:
                print(serial, end="")
                s_cnt += 1

            print("\n")
            print(s_cnt, "serials")
            print("\n")
    #     addr = parsed_data[serial][0].group('Addr')
    #     addr_list = [addr, ]
    #
    #     if addr in Categorized_data:
    #         if pattern in Categorized_data[addr]:
    #             Categorized_data[addr][pattern].append(serial)
    #         else:
    #             Categorized_data[addr][pattern] = [serial, ]
    #     else:
    #         Categorized_data[addr] = {pattern: [serial, ]}
    #
    #
    #     for line in parsed_data[serial]:
    #         if line.group('Addr') not in addr_list:
    #             addr_list.append(line.group('Addr'))
    #
    #             addr = line.group('Addr')
    #
    #             if addr in Categorized_data:
    #                 if pattern in Categorized_data[addr]:
    #                     Categorized_data[addr][pattern].append(serial)
    #                 else:
    #                     Categorized_data[addr][pattern] = [serial, ]
    #             else:
    #                 Categorized_data[addr] = {pattern: [serial, ]}
    #
    #
    # for addr in Categorized_data:
    #     print(" >> ", addr)
    #     cnt = 1
    #     for pattern in Categorized_data[addr]:
    #         #print(pattern)
    #         print("Pattern", cnt, ": ", end = "")
    #         for serial in Categorized_data[addr][pattern]:
    #             print(serial, end = " ")
    #         print()
    #         cnt += 1
    #
    #     print()

    # Categorize with ADDRESS
    elif com == 2:

        for pattern in patterns.keys():

            patterns_op = {}

            for serial in patterns[pattern]:
                pattern_op = ()
                for line in parsed_data[serial]:
                    p = str(line.group('Components'))+"/"+str(line.group('Comp_line'))+"/"+str(line.group('Addr'))
                    pattern_op = pattern_op + (p, )

                if pattern_op in patterns_op:
                    patterns_op[pattern_op].append(serial)
                else:
                    patterns_op[pattern_op] = [serial, ]

            print("\n**************** PATTERN ****************")
            print(pattern)
            for pattern_op in patterns_op.keys():
                print("-------GROUP-------")
                for serial in patterns_op[pattern_op]:
                    print(serial, end = " ")
                print("")

    # Categorize with OPERATION
    elif com == 3:

        for pattern in patterns.keys():

            patterns_op = {}

            for serial in patterns[pattern]:
                pattern_op = ()
                for line in parsed_data[serial]:
                    p = str(line.group('Components'))+"/"+str(line.group('Comp_line'))+"/"+str(line.group('MemMsg'))
                    pattern_op = pattern_op + (p, )

                if pattern_op in patterns_op:
                    patterns_op[pattern_op].append(serial)
                else:
                    patterns_op[pattern_op] = [serial, ]

            print("\n**************** PATTERN ****************")
            print(pattern)
            for pattern_op in patterns_op.keys():
                print("-------GROUP-------")
                for serial in patterns_op[pattern_op]:
                    print(serial, end = " ")
                print("")

if __name__ == "__main__":
    f = open('debug.out', 'r')

    for s in f:
        parsed = pattern.search(s)

        if parsed:
            serial = parsed.group('Serial')
            if serial in parsed_data:
                parsed_data[serial].append(parsed)
            else:
                parsed_data[serial] = [parsed]

    while True:

        print("=========================================================")
        print(" 1. Trace with Seiral")
        print(" 2. Count Unique Serial")
        print(" 3. Print All")
        print(" 4. Print with Serial")
        print(" 5. Categorize with Address")
        print(" 0. Exit")
        print("=========================================================")

        command = int(input("> "))

        # 1. Trace with Seiral
        if command == VISUAL_MODE:

            print("\nEnter Serial to trace.\n  -Add 'a' to show all process. (ex, 272a)\n  -Enter 0 to terminate\n\n")
            while True:
                serial = input("Serial: ")

                if serial == "0":
                    break

                if serial.endswith('a'):
                    serial = serial[0:-1]
                    VISUAL_MODE_ALL = True
                else:
                    VISUAL_MODE_ALL = False

                try:
                    stdscr = curses.initscr()
                    curses.noecho()
                    curses.cbreak()
                    stdscr.keypad(1)
                    stdscr.refresh()
                    visual_print_serial(stdscr, serial)

                    c = stdscr.getch()

                    #End
                    stdscr.clear()
                    curses.nocbreak()
                    stdscr.keypad(0)
                    curses.echo()
                    curses.endwin()

                except:
                    print("Terminated with ERROR")
                    stdscr.clear()
                    curses.nocbreak()
                    stdscr.keypad(0)
                    curses.echo()
                    curses.endwin()

        # 2. Count Unique Serial
        if command == CNT_UNIQUE:
            print("Unique Serial # :".ljust(20), len(parsed_data))

        # 3. Print All
        if command == PRINT_ALL:
            print_all()

        if command == PRINT_SERIAL:
            serial = input("Serial: ")
            print_serial(serial)

        # 4. Categorize with Address
        if command == CATEGORIZE:
            categorize()

        # 0. Exit
        if command == EXIT:
            break
