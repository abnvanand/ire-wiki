tertiary_fp = open('tertiary.txt', "w")
with open('indexes3/secondary-offset.txt') as fp:
    while fp:
        fp_pos = fp.tell()
        line = fp.readline()
        if not line:
            break

        term, primary_offset = line.split("=")
        tertiary_fp.write(f"{term}={fp_pos}\n")

        count = 100
        while line and count:
            count -= 1
            line = fp.readline()

tertiary_fp.close()
