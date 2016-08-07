package com.cloudera.ds.brainwave;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Vector;
import org.apache.hadoopts.app.bucketanalyser.MacroTrackerFrame;
import org.apache.hadoopts.app.bucketanalyser.TSBucketSource;
import org.apache.hadoopts.app.bucketanalyser.TSBucketTransformation;
import org.apache.hadoopts.chart.simple.MultiChart;
import org.apache.hadoopts.data.series.Messreihe;
import org.apache.hadoopts.hadoopts.core.TSBucket;

/**
 * A tool for testing the Brain Signal Classification
 *
 * https://gallery.cortanaintelligence.com/Competition/Decoding-Brain-Signals-2?share=1
 *
 * @author kamir
 * 
 * Inspiration for the project: Peter Whitney, Cloudera
 * 
 * Goal: Demonstration of a use-case for time series based studies on 
 *       physiological data using Hadoop.TS3
 * 
 */
public class ModelTesterCortanaDBS {

    public static void main(String[] args) throws FileNotFoundException, IOException {

        MacroTrackerFrame.init("Cortana - BSD");

        // EXP 1
        String label_of_EXPERIMENT = "CN";
        String subLabel = "BSD";

        String base = "./tsb_" + System.currentTimeMillis();
        File BASE = new File(base);
        BASE.mkdirs();

        Messreihe[][] fullSeries = new Messreihe[64][4];

        String CSVFN = "/Users/kamir/Downloads/ecog_train_with_labels.csv";

        FileReader fr = new FileReader(CSVFN);
        BufferedReader br = new BufferedReader(fr);

        for (int j = 0; j < 4; j++) {

            String person = base + "/p" + (j + 1);
            File PERSON_im = new File(person + "/image");
            File PERSON_bl = new File(person + "/blank");
            PERSON_im.mkdirs();
            PERSON_bl.mkdirs();

            for (int i = 0; i < 64; i++) {

                Messreihe mr = new Messreihe();
                mr.setLabel(i + "_" + j);

                fullSeries[i][j] = mr;

            }
        }

        String header = br.readLine();
        int l = header.split(",").length;

        System.out.println(">>> Step 2");

        try {
 

            while (br.ready()) {

                String tp = br.readLine();
                String[] cells = tp.split(",");

                int j = Integer.parseInt(cells[0].substring(2, 3));

                if ( cells.length < l ) throw new Exception("short line");
                
//                System.out.println( cells[0] );
                
                for (int i = 0; i < 64; i++) {
                    fullSeries[i][j - 1].addValue(Double.parseDouble(cells[i+1]));
                }

//                if ( j > 1 ) br.close();
            }
        } catch (Exception ex) {

        };

        System.out.println(">>> Step 3");

        int PATIENT = 0;
        System.out.println(fullSeries[0][PATIENT].yValues.size());
        
        int eMax = 20; // nr of pairs
        int eMin = 0; // nr of pairs
        
        int length = 400;

        int PATIENT_to_show = 0;

        while (PATIENT < 4) {

            BrainState[] statesImage = new BrainState[eMax];
            BrainState[] statesBlank = new BrainState[eMax];

            for (int k = 0; k < eMax; k = k + 1) {

                // What goes to the bucket???
                statesImage[k] = new BrainState( "p" + PATIENT, k, "image");
                statesBlank[k] = new BrainState( "p" + PATIENT, k, "blank");

            }

            // go over all series with 432 snippets 
            for (int i = 0; i < 64; i++) {

                Messreihe[] snippets = fullSeries[i][PATIENT].split(length, eMax * 2);

                for (int k = 0; k < eMax; k = k + 2) {

                    int t = k/2;
                    
                    Messreihe mr1 = snippets[k];
                    Messreihe mr2 = snippets[k+1];
                    
                    mr1.setLabel(PATIENT + "_e" + i+"_i");
                    mr2.setLabel(PATIENT + "_e" + i+"_b");
                    
                    // What goes to the bucket???
                    statesImage[t].vmr.add( mr1 );
                    statesBlank[t].vmr.add( mr2 );
                }

            }

//            if (PATIENT == PATIENT_to_show) {
                
//                statesImage[0].show();
//                statesBlank[0].show();
                
//                statesImage[0].store();
//                statesBlank[0].store();
                
                for (int k = eMin; k < eMax; k = k + 1) {
//                     What goes to the bucket???
                    statesImage[k].store();
                    statesBlank[k].store();
                }

//            }

            PATIENT++;

        }
        
        System.out.println( ">>> stored: " + BrainState.storedStates );
        System.exit(0);

    }

}

class BrainState {

    static int storedStates = 0;
    
    public BrainState(String n, int i, String c) {
        nr = i;
        patient = n;
        cat = c;
    }

    String patient = "xyz";
    String cat = "abc";
    int nr = -1;

    // images
    public Vector<Messreihe> vmr = new Vector<Messreihe>();

    public void store() throws IOException {

        if ( vmr.size() == 0 ) return;
        
       
        TSBucket b = new TSBucket();
        
        String label = patient;
        
        b.createBucketFromVectorOfSeries(label, cat, vmr);
        
        String comment = "";
        
        String folder = "./"+label + "_" + cat;
        String fn = nr + "_raw";
        File f = new File( folder );
        f.mkdir();
        
        MultiChart.store(vmr, label, "y(t)", "t", true, folder, fn, comment);
        
        storedStates++;

        
    }
        
    public void show() {

        System.out.println(">>> Step 4");

        MacroTrackerFrame.addTransformation(TSBucketTransformation.getTransformation("PATIENT " + nr, cat, "Vector "));

        TSBucketSource t = TSBucketSource.getSource("Vector");
        MacroTrackerFrame.addSource(t);

        BrainSignalCorrelator c1 = BrainSignalCorrelator.getInstance();

        MultiChart.openWithCorrelator(vmr, true, "PATIENT 0 - " + cat, c1);

    }

}
