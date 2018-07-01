package test;

import java.io.Serializable;

public class Counter implements Serializable{                                                         
    private final int id;                                                                             
    private final int m_count;                                                                        
    private final int f_count;                                                                        

    Counter(int id, int m_count, int f_count) {                                                       
        this.id = id;                                                                                 
        this.m_count = m_count;                                                                       
        this.f_count = f_count;                                                                       
    }                                                                                                 

    public String toString() {                                                                        
        return id + " " + m_count + " "  + f_count;                                                   
    }    
    // getters                                                                                          
}        