package org.liu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RecommendDetail implements Serializable {
    public int uid;
    public int mid;
    public double degree;
}
