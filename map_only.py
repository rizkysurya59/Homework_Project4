from mrjob.job import MRJob
import psycopg2

class FilterProduct(MRJob):
    def mapper_init(self):
        #make postgres database available to mapper
        #initial connection
        self.conn = psycopg2.connect(database="project_4", user="postgres", password="sword1st", host="host.docker.internal", port="5432")

    def mapper(self, _, line):
        self.cur = self.conn.cursor()
        item = line.strip().split(',')
        if int(item[3]) > 10:
            self.cur.execute("insert into product (product_id,product_name,product_category,price) values (%s,%s,%s,%s)", 
            (item[0],item[1],item[2],item[3]))
            yield item[1],1

    def mapper_final(self):
        #Close connection always final after init
        self.conn.commit()
        self.conn.close()
        
if __name__ == '__main__':
    FilterProduct.run()