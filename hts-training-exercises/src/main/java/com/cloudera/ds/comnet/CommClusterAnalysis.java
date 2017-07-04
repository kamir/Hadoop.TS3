/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudera.ds.comnet;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;
import org.apache.hadoopts.chart.simple.MultiChart;
import org.apache.hadoopts.data.series.Messreihe;

/**
 *
 * @author kamir
 */
public class CommClusterAnalysis {

    public static void main(String[] args) throws FileNotFoundException, IOException {

 //       File f1 = new File("/__A__/TS_df44_LABELS_L4_host.csv/part-00000");
        File f2 = new File("/__A__/TS_df44_strongcompnum_L4.csv/part-00000");
//        File f3 = new File("/__A__/TS_df45_compnum_L4.csv/part-00000");

  //      File f4 = new File("/__A__/TS_df45_LABELS_L4_host.csv/part-00000");
        File f5 = new File("/__A__/TS_df45_strongcompnum_L4.csv/part-00000");
//        File f6 = new File("/__A__/TS_df44_compnum_L4.csv/part-00000");


        Vector<Integer> select45 = new Vector();
        select45.add(75);
        select45.add(76);
        select45.add(77);
        select45.add(79);
        select45.add(87);


        Vector<Integer> select44 = new Vector();
        select44.add(19);
        select44.add(40);
        select44.add(42);
        select44.add(55);
        select44.add(57);


        Vector<Vector<Integer>> selectors = new Vector();
        selectors.add(select44);
        selectors.add(select45);


        File[] FILES = new File[2];

        int[] BIN = { 5, 5, 1, 1,1,1 };
        int[] SELECTOR = { 0, 1, 1, 1, 2, 2 };

        FILES[0] = f2;
        FILES[1] = f5;
//        FILES[2] = f3;
 //       FILES[2] = f4;
  //      FILES[3] = f5;
//        FILES[5] = f6;
        
        
        String label;
                
        Vector<Messreihe> v3 = new Vector<Messreihe>();

        int i = 0;
        for (File f : FILES) {

            Hashtable<String, Messreihe> vmr = new Hashtable<String, Messreihe>();

            BufferedReader br = new BufferedReader(new FileReader(f));

            String l1 = br.readLine();

            Messreihe mr2 = new Messreihe();

            while (br.ready()) {

                String line = br.readLine();

                // System.out.println(line);
                String[] fields = line.split(",");
                String key = fields[0];

                if (key.equals("null")) {

                } else {
                    Messreihe mr = vmr.get(key);
                    if (mr == null) {
                        
                        mr = new Messreihe();
                        mr.setLabel(key);
                        System.out.println( "*** " + key + " ***");
                        vmr.put(key, mr);
                        
                    }
                    try {

                        double x = Double.parseDouble(fields[1]);
                        double y = Double.parseDouble(fields[2]);

                        mr.addValuePair(x, y);

                    } catch (Exception ex) {

                    }
                }

            }

            Enumeration<Messreihe> en = vmr.elements();

            Vector<Messreihe> v = new Vector<Messreihe>();

            while (en.hasMoreElements()) {

                Messreihe mr = en.nextElement();

                String l = mr.getLabel();
                Integer key = Integer.parseInt( l );

                System.out.println(" ***### " + l + " => " + key + " -> " + SELECTOR[i] + " : " + selectors.get( SELECTOR[i] ) );


                if ( selectors.get( SELECTOR[i] ).contains( key ) ) {
                    System.out.println("JA");
                    v.add(mr);
                }

            }

            double max = getMaxXFromAll(v);

            System.out.println(">>>>> " + max);

            Vector<Messreihe> v2 = new Vector<Messreihe>();

            Enumeration<Messreihe> en2 = v.elements();

            Messreihe mr3 = getZeros( "sum ", max );
           
            
             
            while (en2.hasMoreElements()) {

                Messreihe mr = en2.nextElement();

                if (mr.getYData().length == 0) {
                    mr = getZeros(mr.getLabel(), max);
                } else {
                    mr = patchZeros(mr, max);
                }

//                v2.add(mr);
                v2.add(mr.setBinningX_sum(BIN[i]));

                mr3 = mr3.add( mr.copy() );
                
                System.out.println(mr.getLabel() + " => " + mr.getMaxX() + " : " + mr.getYData().length);
//                v.add( mr.setBinningX_sum(15) );
            }

            v3.add(mr3);

            
            Correlator correlator = new Correlator();
            label = f.getParentFile().getName();

            MultiChart.open(v2, true, label);

            //correlator.calcSingleBucketCorrelations(v2, label);
            i++;
        }
        
         MultiChart.open(v3, true, "all");
    }

    public static double getMaxXFromAll(Vector<Messreihe> v) {
        int i = 1;
        double y = v.get(0).getMaxX();
        while (i < v.size()) {
            double y2 = v.get(i).getMaxX();
            if (y2 > y) {
                y = y2;
            }
            i++;
        }

        return y;
    }

    private static Messreihe patchZeros(Messreihe mr1, double max) {

        Messreihe mr = new Messreihe();
        mr.setLabel(mr1.getLabel());

        int j = 0;
        double[][] d = mr1.getData();

        double x = d[0][j];
        double y = d[1][j];

        for (int i = 1; i <= max; i++) {
            if (x == i) {
                mr.addValuePair(x, y);

                j++;
                if (j < mr1.getYData().length) {
                    x = d[0][j];
                    y = d[1][j];
                }
            } 
            else {
                mr.addValuePair(i, 0);
            }
        }

        return mr;

    }

    private static Messreihe getZeros(String label, double max) {

        Messreihe mr = new Messreihe();
        mr.setLabel(label);
        for (int i = 1; i <= max; i++) {
            mr.addValuePair(i, 0);
        }

        return mr;

    }
}
