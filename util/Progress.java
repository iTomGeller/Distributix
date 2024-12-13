package Distributix.util;

import java.util.ArrayList;

public class Progress {
  private String status = "";
  private float progress;
  private int currentPhase;
  private ArrayList phases = new ArrayList<>();
  private Progress parent;
  private float progressPerPhase;

  public Progress() {}

  public Progress addPhase(String status) {
    Progress phase = addPhase();
    phase.setStatus(status);
    return phase;
  }

  public Progress addPhase() {
    Progress phase = new Progress();
    phases.add(phase);
    phase.parent = this;
    progressPerPhase = 1.0f / (float)phases.size();
    return phase;
  }

  public void startNextPhase() {
    currentPhase++;
  }

  public Progress phase() {
    return (Progress)phases.get(currentPhase);
  }

  public void complete() {
    progress = 1.0f;
    if (parent != null) {
      startNextPhase();
    }
  }

  public void set(float progress) {
    this.progress = progress;
  }

  public float get() {
    Progress node = this;
    while (node.parent != null) {
      node = node.parent;
    }
    return node.getInternal();
  }

  /** Computes progress in this node. */
  private float getInternal() {
    int phasecount = phases.size();
    if (phasecount != 0) {
      float subProgress = currentPhase < phasecount ? phase().getInternal() : 0.0f;
      return progressPerPhase * (currentPhase + subProgress);
    } else {
      return progress;
    }
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String toString() {
    StringBuffer result = new StringBuffer();
    toString(result);
    return result.toString();
  }

  private void toString(StringBuffer buffer) {
    buffer.append(status);
    if (phases.size() != 0 && currentPhase < phases.size()) {
      buffer.append(" > ");
      phase.toString(buffer);//递归把子节点也转换成string
    }
  }
}
