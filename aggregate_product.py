from mrjob.job import MRJob, MRStep
import psycopg2

class AggregateProduct(MRJob):

    # def reducer_init(self):
    #     #make postgres database available to mapper
    #     #initial connection
    #     self.conn = psycopg2.connect(database="project_4", user="postgres", password="sword1st", host="localhost", port="5432")
    #     # host.docker.internal

    def mapper(self, _, line):
        item = line.strip().split(',')
        year = item[1][-4:]
        yield item[1], int(item[4])

    def reducer(self, key, values):        
        yield key,sum(values)
        # self.cur = self.conn.cursor()
        # self.cur.execute("insert into total_order_yearly (year,total_order) values (%s,%s)", 
        

#Homework
    def mapper2(self, _, line):
        productitem = line.strip().split(',')
        yield productitem[3], 1
    
    def reducer2(self, key, values):
        yield key, sum(values)

    #homework another ways
    def homework(self, _, line):
        passitem = line.strip().split(',')
        for item in passitem:
            yield item[1], item[3]

    def reducer3(self, key, values):
        yield key, values

    # def store(self, key, values):
            # self.cur = self.conn.cursor()
            # self.cur.execute("insert into total_order_yearly (year,total_order) values (%s,%s)", 
            # (key, values))

    def steps(self):
        return [
            MRStep(
                #    mapper=self.mapper,  
                   mapper=self.mapper,
                #    reducer_init=self.reducer_init,                 
                   reducer=self.reducer
                #    reducer=self.reducer2
                #    reducer_final=self.reducer_final
                   ),
            # MRStep(mapper=self.mapper,
            # reducer=self.reducer2
            #     reducer_init=self.reducer_init,
            # reducer=self.store,
            # reducer_final=self.reducer_final
            # )
        ]

    # def reducer_final(self):
        #Close connection always final after init
        # self.conn.commit()
        # self.conn.close()
        
if __name__ == '__main__':
    AggregateProduct.run()