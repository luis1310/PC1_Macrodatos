/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package covidquery6;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

/**
 * Comparador personalizado para ordenar por densidad
 * @author luisdelacruzmantilla
 */
public class DensidadComparator extends WritableComparator {
    
    public DensidadComparator() {
        super(Text.class, true);
    }
    
    @Override
    public int compare(Object a, Object b) {
        Text t1 = (Text) a;
        Text t2 = (Text) b;
        
        String s1 = t1.toString();
        String s2 = t2.toString();
        
        // Definir orden personalizado: ALTA -> MEDIA -> BAJA
        int priority1 = getPriority(s1);
        int priority2 = getPriority(s2);
        
        return Integer.compare(priority1, priority2);
    }
    
    private int getPriority(String densidad) {
        if (densidad.equals("ALTA_DENSIDAD")) {
            return 1;
        } else if (densidad.equals("MEDIA_DENSIDAD")) {
            return 2;
        } else if (densidad.equals("BAJA_DENSIDAD")) {
            return 3;
        } else {
            return 4; // Para cualquier otro caso
        }
    }
}