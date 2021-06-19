with open("flattened.json", 'a') as outfile:
    for d in js:
        json.dump(d, outfile)
        outfile.write('\n')