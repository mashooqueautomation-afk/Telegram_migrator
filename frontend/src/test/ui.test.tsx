import { describe, expect, it } from 'vitest';
import { render, screen } from '@testing-library/react';
import React from 'react';

function StatusChip({ value }: { value: string }) {
  return <span className={`badge ${value === 'COMPLETED' ? 'green' : 'blue'}`}>{value.toLowerCase()}</span>;
}

describe('frontend smoke components', () => {
  it('renders a completed status as readable text', () => {
    render(<StatusChip value="COMPLETED" />);
    expect(screen.getByText('completed')).toBeInTheDocument();
  });
  it('renders running status without hiding the state name', () => {
    render(<StatusChip value="RUNNING" />);
    expect(screen.getByText('running')).toBeInTheDocument();
  });
});
